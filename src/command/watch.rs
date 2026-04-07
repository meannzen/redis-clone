use bytes::Bytes;

use crate::{parse::Parse, Connection, Frame};

#[derive(Debug)]
pub struct Watch {
    args: Vec<Bytes>,
}

impl Watch {
    pub fn new(args: Vec<Bytes>) -> Self {
        Watch { args }
    }

    pub fn parse_frame(parse: &mut Parse) -> crate::Result<Watch> {
        let mut args: Vec<Bytes> = Vec::new();
        while let Ok(v) = parse.next_bytes() {
            args.push(v);
        }

        Ok(Watch { args })
    }

    pub async fn apply(self, conn: &mut Connection) -> crate::Result<()> {
        dbg!(self.args);
        let frame = Frame::Simple("OK".to_string());
        conn.write_frame(&frame).await?;
        Ok(())
    }
}
