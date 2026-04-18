use crate::{server::WatchRegistry, Connection};

#[derive(Debug)]
pub struct Unwatch;

impl Unwatch {
    pub async fn apply(
        self,
        watch_registery: &WatchRegistry,
        conn: &mut Connection,
    ) -> crate::Result<()> {
        {
            let mut watch_keys = watch_registery.lock().unwrap();
            watch_keys.clear();
        }
        conn.write_frame(&crate::Frame::Simple("OK".to_string()))
            .await?;
        Ok(())
    }
}
