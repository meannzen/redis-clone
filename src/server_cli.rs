use clap::Parser;
use std::env;

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

    pub fn port(&self) -> u16 {
        self.port.unwrap_or(DEFAULT_PORT)
    }
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
