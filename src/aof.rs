use crate::{
    connection::parse_frame_from_buffer,
    server_cli::{get_aof_incremental_path, Cli},
    store::Db,
    Command,
};
use bytes::BytesMut;
use std::sync::Arc;

pub async fn replay_aof(config: &Arc<Cli>, db: &Db) -> crate::Result<()> {
    let path = match get_aof_incremental_path(config).await {
        Ok(p) => p,
        Err(_) => {
            println!("No AOF manifest found, skipping replay.");
            return Ok(());
        }
    };

    if !std::path::Path::new(&path).exists() {
        println!("AOF file {} not found, skipping replay.", path);
        return Ok(());
    }

    let data = tokio::fs::read(&path).await?;
    let mut buffer = BytesMut::from(&data[..]);

    println!("Replaying AOF from {} ({} bytes)...", path, data.len());

    let mut count = 0;
    while !buffer.is_empty() {
        if let Some(frame) = parse_frame_from_buffer(&mut buffer)? {
            let command = Command::from_frame(frame)?;

            command.apply_locally(db).await?;
            count += 1;
        } else {
            eprintln!("AOF replay ended early: incomplete or corrupt frame at end of file.");
            break;
        }
    }

    println!("AOF replay complete. {} commands applied.", count);
    Ok(())
}
