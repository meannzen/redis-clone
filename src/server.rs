use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc, Semaphore},
};
const MAX_CONNECTIONS: usize = 250;

#[derive(Debug, Clone)]
pub struct ReplicaState {
    pub connections: Arc<Mutex<Vec<Connection>>>,
    pub offset: Arc<Mutex<u64>>,
    pub acked: Arc<Mutex<u64>>,
}

#[derive(Debug)]
pub enum QueueCommand {
    SET(Set),
    INCR(Incr),
    GET(Get),
}

#[derive(Debug, Clone)]
pub struct TransactionState {
    pub multi: Arc<Mutex<bool>>,
    pub queue_command: Arc<Mutex<VecDeque<QueueCommand>>>,
}

impl Default for TransactionState {
    fn default() -> Self {
        Self {
            multi: Arc::new(Mutex::new(false)),
            queue_command: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

impl Default for ReplicaState {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicaState {
    pub fn new() -> Self {
        Self {
            connections: Arc::new(Mutex::new(Vec::new())),
            offset: Arc::new(Mutex::new(0)),
            acked: Arc::new(Mutex::new(0)),
        }
    }
}

pub type WatchRegistry = Arc<Mutex<HashMap<String, bool>>>;

use crate::{
    aof::replay_aof,
    command::{Get, Incr, Set},
    database::parser::RdbParse,
    server_cli::{get_aof_incremental_path, Cli},
    store::{Db, Store},
    Command, Connection,
};
#[derive(Debug)]
struct Listener {
    listener: TcpListener,
    store: Store,
    config: Arc<Cli>,
    limit_connection: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
    replica_state: ReplicaState,
    watch_registry: WatchRegistry,
}

impl Listener {
    async fn run(&self) -> crate::Result<()> {
        loop {
            let permit = self.limit_connection.clone().acquire_owned().await.unwrap();
            let socket = self.accept().await?;

            let mut connection = Connection::new(socket);
            if self.store.db.get_user_password_hash("default").is_some() {
                connection.set_authenticated(false, None);
            }
            let mut handler = Handler {
                db: self.store.db.clone(),
                config: self.config.clone(),
                connection,
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
                replica_state: self.replica_state.clone(),
                transaction_state: TransactionState::default(),
                watch_registry: self.watch_registry.clone(),
            };

            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    println!("{:?} connection failed", err);
                }
                drop(permit);
            });
        }
    }

    async fn accept(&self) -> crate::Result<TcpStream> {
        let mut backoff = 1;
        loop {
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        return Err(err.into());
                    }

                    eprintln!("accept failed, backing off for {}s: {:?}", backoff, err);
                    tokio::time::sleep(Duration::from_secs(backoff)).await;

                    backoff *= 2;
                }
            }
        }
    }
}

pub async fn run(listener: TcpListener, config: Cli, shutdown: impl Future) {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

    let store = Store::new();
    if let Some(file) = config.file_path() {
        let path = Path::new(&file);
        if path.exists() {
            let database = RdbParse::parse(&file).unwrap();
            for (key, value) in database.entries {
                store.db.set(key, value.data, value.expire);
            }
        }
    }

    if config.appendonly == Some("yes".to_string()) {
        if let Err(e) = replay_aof(&Arc::new(config.clone()), &store.db).await {
            eprintln!("Failed to replay AOF: {:?}", e);
        }
    }

    let server = Listener {
        listener,
        store,
        limit_connection: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
        config: Arc::new(config),
        replica_state: ReplicaState::new(),
        watch_registry: Arc::new(Mutex::new(HashMap::new())),
    };

    tokio::select! {
        res = server.run() => {
           if let Err(err) = res {
               println!("{:?} failed to accept", err);
           }
        }
        _ = shutdown => {
            println!("shutting down");
        }
    }

    let Listener {
        notify_shutdown,
        shutdown_complete_tx,
        ..
    } = server;
    drop(notify_shutdown);
    drop(shutdown_complete_tx);

    let _ = shutdown_complete_rx.recv().await;
}

#[derive(Debug)]
struct Handler {
    db: Db,
    config: Arc<Cli>,
    connection: Connection,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
    replica_state: ReplicaState,
    transaction_state: TransactionState,
    watch_registry: WatchRegistry,
}

impl Handler {
    async fn run(&mut self) -> crate::Result<()> {
        loop {
            let frame_opt = tokio::select! {
                res = self.connection.read_frame() => res,
                _ = self.shutdown.recv() => {
                    return Ok(());
                }
            };

            let frame = match frame_opt? {
                Some(frame) => frame,
                None => return Ok(()),
            };

            let (frame, raw_bytes) = frame;

            let command = Command::from_frame(frame.clone())?;
            let is_writer = command.is_writer();
            command
                .apply(
                    &self.transaction_state,
                    &self.replica_state,
                    &self.db,
                    &self.config,
                    &mut self.connection,
                    &mut self.shutdown,
                    &self.watch_registry,
                )
                .await?;

            if is_writer && self.config.appendonly == Some("yes".to_string()) {
                let path = get_aof_incremental_path(&self.config).await?;
                let mut file = tokio::fs::OpenOptions::new()
                    .append(true)
                    .open(path)
                    .await?;
                file.write_all(&raw_bytes).await?;
                file.sync_all().await?;
            }

            if is_writer {
                let frame_len = frame.clone().to_vec().len() as u64;
                {
                    let mut off_guard = self.replica_state.offset.lock().unwrap_or_else(|poison| {
                        eprintln!("Replica lock poisoned: {:?}", poison);
                        std::process::exit(1);
                    });
                    *off_guard += frame_len;
                }
                let replicas: Vec<Connection> = {
                    let guard = self
                        .replica_state
                        .connections
                        .lock()
                        .unwrap_or_else(|poison| {
                            eprintln!("Replica lock poisoned: {:?}", poison);
                            std::process::exit(1);
                        });
                    guard
                        .iter()
                        .filter_map(|stream| stream.try_clone().ok())
                        .collect()
                };
                let frame = frame.clone();
                tokio::spawn(async move {
                    for mut stream in replicas {
                        if let Err(e) = stream.write_frame(&frame).await {
                            eprintln!("Failed to broadcast frame to replica: {:?}", e);
                        }
                    }
                });
            }
        }
    }
}

#[derive(Debug)]
pub struct Shutdown {
    is_shutdown: bool,
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    pub fn new(notify: broadcast::Receiver<()>) -> Self {
        Shutdown {
            is_shutdown: false,
            notify,
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    pub async fn recv(&mut self) {
        if self.is_shutdown {
            return;
        }

        let _ = self.notify.recv().await;
        self.is_shutdown = true;
    }
}
