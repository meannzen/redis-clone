use clap::Parser;
use redis_starter_rust::{
    clients::Client,
    server,
    server_cli::{Cli, ReplicaOf},
    Result,
};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Cli::parse();

    let server_cli = config.clone();
    server_cli.set_up_aof_persistence()?;
    let server_port = server_cli.port();
    let server_handle = tokio::spawn(async move {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", server_port))
            .await
            .expect("Failed to bind listener");
        server::run(listener, server_cli, tokio::signal::ctrl_c()).await
    });

    if let Some(ReplicaOf {
        host,
        port: master_port,
    }) = config.replicaof
    {
        tokio::spawn(async move {
            let mut client = Client::connect(format!("{}:{}", host, master_port))
                .await
                .expect("Failed to connect");
            tokio::select! {
               res =  Client::replica(&mut client, server_port)=> res.expect("Failed to handsack"),
               _= tokio::signal::ctrl_c() => {
               }
            }
        });
    }

    server_handle.await?;
    Ok(())
}
