use core::clone::Clone;

use bytes::Bytes;

use crate::{parse::Parse, Connection, Frame};

#[derive(Debug)]
pub struct Config {
    cmd: String,
}

impl Config {
    pub fn new(cmd: impl ToString) -> Self {
        Config {
            cmd: cmd.to_string(),
        }
    }

    pub fn cmd(&self) -> &str {
        &self.cmd
    }

    pub fn parse_frame(parse: &mut Parse) -> crate::Result<Config> {
        let cmd = parse.next_string()?;
        Ok(Config { cmd })
    }

    pub async fn apply(
        self,
        config: &crate::server_cli::Cli,
        conn: &mut Connection,
    ) -> crate::Result<()> {
        let mut frame: Frame = Frame::array();

        match self.cmd.as_str() {
            "dir" => {
                frame.push_bulk(Bytes::from("dir"));
                let directory = config
                    .dir
                    .clone()
                    .unwrap_or_else(|| config.get_current_dir());
                frame.push_bulk(Bytes::from(directory));
            }
            "dbfilename" => {
                if let Some(filename) = config.dbfilename.clone() {
                    frame.push_bulk(Bytes::from("dbfilename"));
                    frame.push_bulk(Bytes::from(filename));
                }
            }
            "appendonly" => {
                frame.push_bulk(Bytes::from("appendonly"));
                if let Some(append_only) = config.appendonly.clone() {
                    frame.push_bulk(Bytes::from(append_only));
                } else {
                    frame.push_bulk(Bytes::from("no"));
                }
            }
            "appenddirname" => {
                frame.push_bulk(Bytes::from("appenddirname"));
                if let Some(append_dir_name) = config.appenddirname.clone() {
                    frame.push_bulk(Bytes::from(append_dir_name));
                } else {
                    frame.push_bulk(Bytes::from("appendonlydir"));
                }
            }
            "appendfilename" => {
                frame.push_bulk(Bytes::from("appendfilename"));
                if let Some(append_file_name) = config.appendfilename.clone() {
                    frame.push_bulk(Bytes::from(append_file_name));
                } else {
                    frame.push_bulk(Bytes::from("appendonly.aof"));
                }
            }
            "appendfsync" => {
                frame.push_bulk(Bytes::from("appendfsync"));
                if let Some(appendfsync) = config.appendfsync.clone() {
                    frame.push_bulk(Bytes::from(appendfsync));
                } else {
                    frame.push_bulk(Bytes::from("everysec"));
                }
            }
            _ => {
                // Handle unknown config keys if necessary,
            }
        }
        conn.write_frame(&frame).await?;
        Ok(())
    }
}
