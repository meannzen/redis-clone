use clap::Parser;
use std::io::Write;
use std::{env, fs, io};

use crate::DEFAULT_PORT;
#[derive(Debug, Clone)]
pub struct ReplicaOf {
    pub host: String,
    pub port: u16,
}
#[derive(Parser, Debug, Clone)]
#[command(
    name = "rdis-server",
    version,
    author,
    about = "A redis config serverr"
)]
pub struct Cli {
    #[arg(long)]
    port: Option<u16>,
    #[arg(long, value_parser = clap::value_parser!(ReplicaOf))]
    pub replicaof: Option<ReplicaOf>,
    #[arg(long)]
    pub dir: Option<String>,
    #[arg(long)]
    pub dbfilename: Option<String>,
    #[arg(long)]
    pub appendonly: Option<String>,
    #[arg(long)]
    pub appenddirname: Option<String>,
    #[arg(long)]
    pub appendfilename: Option<String>,
    #[arg(long)]
    pub appendfsync: Option<String>,
}

impl Cli {
    pub fn file_path(&self) -> Option<String> {
        let file_name = match &self.dbfilename {
            Some(f) => f.as_str(),
            None => return None,
        };

        let dir_path = match &self.dir {
            Some(dir) => dir.clone(),
            None => env::current_dir()
                .ok()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|| ".".to_string()),
        };

        Some(format!("{}/{}", dir_path.trim_end_matches('/'), file_name))
    }

    pub fn get_current_dir(&self) -> String {
        env::current_dir()
            .ok()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| ".".to_string())
    }

    fn append_only(&self) -> bool {
        matches!(&self.appendonly, Some(v) if v == "yes")
    }

    pub fn set_up_aof_persistence(&self) -> io::Result<()> {
        if !self.append_only() {
            return Ok(());
        }

        let dir_path = self.dir.clone().unwrap_or_else(|| self.get_current_dir());
        let append_dir_name = self.appenddirname.as_deref().unwrap_or("appendonlydir");
        let base_file_name = self.appendfilename.as_deref().unwrap_or("appendonly.aof");

        let full_append_dir = format!("{}/{}", dir_path.trim_end_matches('/'), append_dir_name);
        fs::create_dir_all(&full_append_dir)?;

        let incr_file_name = format!("{}.1.incr.aof", base_file_name);
        let incr_file_path = format!("{}/{}", full_append_dir, incr_file_name);

        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&incr_file_path)
        {
            Ok(_) => println!("AOF incremental file created: {}", incr_file_name),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => (),
            Err(e) => return Err(e),
        }

        let manifest_path = format!("{}/{}.manifest", full_append_dir, base_file_name);
        let manifest_content = format!("file {} seq 1 type i\n", incr_file_name);

        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&manifest_path)
        {
            Ok(mut file) => {
                file.write_all(manifest_content.as_bytes())?;
                println!("Manifest created at: {}", manifest_path);
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => (),
            Err(e) => return Err(e),
        }

        Ok(())
    }

    pub fn port(&self) -> u16 {
        self.port.unwrap_or(DEFAULT_PORT)
    }
}

pub async fn get_aof_incremental_path(config: &Cli) -> crate::Result<String> {
    let dir = config.dir.as_deref().unwrap_or(".");
    let append_dir = config.appenddirname.as_deref().unwrap_or("appendonlydir");

    let base_name = config.appendfilename.as_deref().unwrap_or("appendonly.aof");

    let manifest_path = format!("{}/{}/{}.manifest", dir, append_dir, base_name);

    let content = tokio::fs::read_to_string(&manifest_path).await?;

    for line in content.lines() {
        // Multi-part AOF uses 'type i' for incremental files
        if line.contains("type i") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                return Ok(format!("{}/{}/{}", dir, append_dir, parts[1]));
            }
        }
    }

    Err("No incremental file found in manifest".into())
}

impl std::str::FromStr for ReplicaOf {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split_whitespace().collect();
        if parts.len() != 2 {
            return Err(format!(
                "Invalid --replicaof format: expected 'HOST PORT', got '{}'",
                s
            ));
        }
        let host = parts[0].to_string();
        let port: u16 = parts[1]
            .parse()
            .map_err(|e| format!("Invalid port '{}': {}", parts[1], e))?;
        Ok(ReplicaOf { host, port })
    }
}
