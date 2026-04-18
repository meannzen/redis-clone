use bytes::Bytes;

use crate::{
    parse::Parse,
    server::{QueueCommand, TransactionState, WatchRegistry},
    store::Db,
    Connection, Frame,
};

#[derive(Debug)]
pub struct Exec;

impl Exec {
    // temporary not sure next is parse or not
    pub fn parse_frame(_parse: &mut Parse) -> crate::Result<Exec> {
        Ok(Exec)
    }

    pub async fn apply(
        self,
        db: &Db,
        trans: &TransactionState,
        conn: &mut Connection,
        watch_registry: &WatchRegistry,
    ) -> crate::Result<()> {
        use atoi::atoi;
        let mut frame;
        let is_dirty;
        {
            let mut multi = trans.multi.lock().unwrap();
            let mut queue_commands = trans.queue_command.lock().unwrap();
            frame = Frame::array();
            let mut watch_keys = watch_registry.lock().unwrap();
            is_dirty = watch_keys.values().any(|v| *v);
            if is_dirty {
                queue_commands.clear();
                watch_keys.clear();
                *multi = false;
            }
            if *multi {
                while let Some(queue_command) = queue_commands.pop_front() {
                    match queue_command {
                        QueueCommand::GET(cmd) => {
                            if let Some(value) = db.get(cmd.key()) {
                                frame.push_bulk(value);
                            } else if let Frame::Array(ref mut v) = frame {
                                v.push(Frame::Null);
                            }
                        }

                        QueueCommand::SET(cmd) => {
                            db.set(cmd.key().to_string(), cmd.value(), cmd.expire());
                            if let Frame::Array(ref mut v) = frame {
                                v.push(Frame::Simple("OK".to_string()));
                            }
                        }
                        QueueCommand::INCR(cmd) => {
                            if let Some(value) = db.get(cmd.key()) {
                                if let Some(mut value) = atoi::<u64>(&value) {
                                    value += 1;
                                    db.set(
                                        cmd.key().to_string(),
                                        Bytes::from(value.to_string()),
                                        None,
                                    );
                                    if let Frame::Array(ref mut v) = frame {
                                        v.push(Frame::Integer(value));
                                    }
                                } else if let Frame::Array(ref mut v) = frame {
                                    v.push(Frame::Error(
                                        "ERR value is not an integer or out of range".to_string(),
                                    ));
                                }
                            } else {
                                db.set(cmd.key().to_string(), Bytes::from("1"), None);
                                if let Frame::Array(ref mut v) = frame {
                                    v.push(Frame::Integer(1));
                                }
                            }
                        }
                    }
                }
                *multi = false;
            } else if !is_dirty {
                frame = Frame::Error("ERR EXEC without MULTI".to_string());
            }
        }
        if !is_dirty {
            conn.write_frame(&frame).await?;
        } else {
            conn.write_null_array().await?;
        }
        Ok(())
    }
}
