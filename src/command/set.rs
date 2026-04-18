use std::time::Duration;

use bytes::Bytes;

use crate::{
    parse::Parse,
    server::{QueueCommand, TransactionState, WatchRegistry},
    store::Db,
    Connection, Frame,
};

#[derive(Debug, Clone)]
pub struct Set {
    key: String,
    value: Bytes,
    expire: Option<Duration>,
}

impl Set {
    pub fn new(key: impl ToString, value: Bytes, expire: Option<Duration>) -> Self {
        Set {
            key: key.to_string(),
            value,
            expire,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> Bytes {
        self.value.clone()
    }

    pub fn expire(&self) -> Option<Duration> {
        self.expire
    }

    pub fn parse_frame(parse: &mut Parse) -> crate::Result<Set> {
        let key = parse.next_string()?;
        let value = parse.next_bytes()?;
        let mut expire = None;
        match parse.next_string() {
            Ok(s) if s.to_uppercase() == "EX" => {
                let secs = parse.next_int()?;
                expire = Some(Duration::from_secs(secs));
            }
            Ok(s) if s.to_uppercase() == "PX" => {
                let mili = parse.next_int()?;
                expire = Some(Duration::from_millis(mili));
            }

            Ok(_) => return Err("currently `SET` only supports the expiration option".into()),
            Err(crate::parse::ParseError::EndOfStream) => {}
            Err(err) => return Err(err.into()),
        }
        Ok(Set { key, value, expire })
    }

    pub async fn apply(
        self,
        db: &Db,
        conn: &mut Connection,
        trans: &TransactionState,
        watch_registry: &WatchRegistry,
    ) -> crate::Result<()> {
        let mut response_str = "OK";
        let mut is_queue = false;
        {
            let multi = trans.multi.lock().unwrap();
            if *multi {
                is_queue = true;
                let mut queue_command = trans.queue_command.lock().unwrap();
                response_str = "QUEUED";
                queue_command.push_back(QueueCommand::SET(self.clone()));
            }
        }
        if !is_queue {
            let mut watch_keys = watch_registry.lock().unwrap();
            watch_keys.entry(self.key.clone()).and_modify(|v| *v = true);
            db.set(self.key, self.value, self.expire);
        }
        let response = Frame::Simple(response_str.to_string());
        conn.write_frame(&response).await?;
        Ok(())
    }

    pub fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("set"));
        frame.push_bulk(Bytes::from(self.key));
        frame.push_bulk(self.value);

        if let Some(ms) = self.expire {
            frame.push_bulk(Bytes::from("px"));
            frame.push_int(ms.as_millis() as u64);
        }

        frame
    }
}
