use crate::{
    parse::Parse,
    server::{TransactionState, WatchRegistry},
    Connection, Frame,
};

#[derive(Debug)]
pub struct Watch {
    args: Vec<String>,
}

impl Watch {
    pub fn new(args: Vec<String>) -> Self {
        Watch { args }
    }

    pub fn parse_frame(parse: &mut Parse) -> crate::Result<Watch> {
        let mut args: Vec<String> = Vec::new();
        while let Ok(v) = parse.next_string() {
            args.push(v);
        }

        Ok(Watch { args })
    }

    pub async fn apply(
        self,
        transaction: &TransactionState,
        conn: &mut Connection,
        watch_registry: &WatchRegistry,
    ) -> crate::Result<()> {
        let mut frame = Frame::Simple("OK".to_string());
        if *transaction.multi.lock().unwrap() {
            frame = Frame::Error("ERR WATCH inside MULTI is not allowed".to_string());
        }
        for key in self.args {
            let mut watch_keys = watch_registry.lock().unwrap();
            watch_keys.insert(key, false);
        }
        conn.write_frame(&frame).await?;
        Ok(())
    }
}
