use std::collections::VecDeque;

use crate::{
    parse::Parse,
    server::{TransactionState, WatchRegistry},
    Connection, Frame,
};

#[derive(Debug)]
pub struct Discard;

impl Discard {
    pub fn parse_frame(_parse: &mut Parse) -> crate::Result<Discard> {
        Ok(Discard)
    }

    pub async fn apply(
        self,
        conn: &mut Connection,
        trans: &TransactionState,
        watch_registery: &WatchRegistry,
    ) -> crate::Result<()> {
        let frame;
        {
            let mut multi = trans.multi.lock().unwrap();
            let mut queue_command = trans.queue_command.lock().unwrap();
            let mut watch_keys = watch_registery.lock().unwrap();
            if *multi {
                queue_command.clear();
                watch_keys.clear();
                frame = Frame::Simple("OK".to_string());
                *multi = false;
            } else {
                frame = Frame::Error("ERR DISCARD without MULTI".to_string());
            }
        }
        conn.write_frame(&frame).await?;
        Ok(())
    }
}
