use std::{
    io::{self, Cursor},
    os::fd::{AsRawFd, FromRawFd},
};

use crate::frame::{self, Frame};
use bytes::{Buf, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
    len: usize,
    authenticated: bool,
    username: Option<String>,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Connection {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(4 * 1024),
            len: 0,
            authenticated: true,
            username: Some("default".to_string()),
        }
    }
    pub fn set_authenticated(&mut self, authenticated: bool, username: Option<String>) {
        self.authenticated = authenticated;
        self.username = username;
    }

    pub fn is_authenticated(&self) -> bool {
        self.authenticated
    }

    pub fn username(&self) -> Option<String> {
        self.username.clone()
    }

    pub async fn read_frame(&mut self) -> crate::Result<Option<(Frame, bytes::Bytes)>> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    /// Reads an RDB file from the connection stream.
    ///
    /// The RDB file is expected in the format `$<length>\r\n<contents>`. This function
    /// handles the streaming nature of TCP by repeatedly reading from the socket until
    /// the complete RDB file, as specified by its length header, has been received.
    pub async fn read_file(&mut self) -> crate::Result<()> {
        loop {
            if let Some((length, header_len)) = self.parse_rdb_header()? {
                let total_len = header_len + length;
                if self.buffer.len() >= total_len {
                    self.buffer.advance(total_len);
                    return Ok(());
                }
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                return if self.buffer.is_empty() {
                    Ok(())
                } else {
                    Err("connection reset by peer".into())
                };
            }
        }
    }

    /// Parses the header of an RDB file to extract its length.
    ///
    /// The header is expected to be in the format `$<length>\r\n`.
    ///
    /// # Returns
    ///
    /// - `Ok(Some((length, header_len)))` if the header is fully present in the buffer.
    ///   `length` is the size of the RDB content, and `header_len` is the size of the
    ///   `$<length>\r\n` header itself.
    /// - `Ok(None)` if the buffer does not yet contain a complete header.
    /// - `Err` if the header is malformed.
    fn parse_rdb_header(&self) -> crate::Result<Option<(usize, usize)>> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        if self.buffer[0] != b'$' {
            return Err("protocol error; expected '$' for RDB file".into());
        }

        if let Some(pos) = self.buffer[1..].windows(2).position(|w| w == b"\r\n") {
            let line_end = 1 + pos;
            let len_bytes = &self.buffer[1..line_end];
            let len_str = std::str::from_utf8(len_bytes)?;
            let length = len_str.parse::<usize>()?;
            let header_len = line_end + 2;
            Ok(Some((length, header_len)))
        } else {
            Ok(None)
        }
    }

    pub fn get_len(&self) -> usize {
        self.len
    }

    pub fn get_client_port(&self) -> io::Result<u16> {
        let inner = self.stream.get_ref();
        Ok(inner.peer_addr()?.port())
    }

    pub fn try_clone(&self) -> io::Result<Self> {
        let inner_stream = self.stream.get_ref();
        let fd = inner_stream.as_raw_fd();

        let dup_fd = unsafe { libc::dup(fd) };
        if dup_fd == -1 {
            return Err(io::Error::last_os_error());
        }

        let dup_std_stream = unsafe { std::net::TcpStream::from_raw_fd(dup_fd) };

        let dup_tokio_stream = TcpStream::from_std(dup_std_stream).map_err(io::Error::other)?;

        Ok(Connection::new(dup_tokio_stream))
    }

    fn parse_frame(&mut self) -> crate::Result<Option<(Frame, bytes::Bytes)>> {
        use frame::Error::Incomplete;

        let mut buff = Cursor::new(&self.buffer[..]);
        match Frame::check(&mut buff) {
            Ok(_) => {
                let len = buff.position() as usize;
                buff.set_position(0);

                self.len = len;

                let raw_bytes = self.buffer.copy_to_bytes(len);

                let mut captured_buff = Cursor::new(&raw_bytes[..]);
                let frame = Frame::parse(&mut captured_buff)?;

                Ok(Some((frame, raw_bytes)))
            }
            Err(Incomplete) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Array(val) => {
                self.stream.write_u8(b'*').await?;
                self.write_decimal(val.len() as u64).await?;
                for entry in &**val {
                    self.write_value(entry).await?;
                }
            }
            _ => self.write_value(frame).await?,
        }

        self.stream.flush().await
    }

    pub async fn write_null_array(&mut self) -> io::Result<()> {
        self.stream.write_all(b"*-1\r\n").await?;
        self.stream.flush().await
    }

    pub async fn write_content_file(&mut self, content: Vec<u8>) -> io::Result<()> {
        self.stream.write_u8(b'$').await?;
        self.stream
            .write_all(content.len().to_string().as_bytes())
            .await?;

        self.stream.write_all(b"\r\n").await?;
        self.stream.write_all(&content).await?;
        self.stream.flush().await?;
        Ok(())
    }

    pub async fn write_geopos(&mut self, positions: Vec<Option<(f64, f64)>>) -> io::Result<()> {
        self.stream.write_u8(b'*').await?;
        self.write_decimal(positions.len() as u64).await?;

        for pos in positions {
            match pos {
                Some((lon, lat)) => {
                    self.stream.write_all(b"*2\r\n").await?;

                    let lon_str = lon.to_string();
                    self.stream.write_u8(b'$').await?;
                    self.write_decimal(lon_str.len() as u64).await?;
                    self.stream.write_all(lon_str.as_bytes()).await?;
                    self.stream.write_all(b"\r\n").await?;

                    let lat_str = lat.to_string();
                    self.stream.write_u8(b'$').await?;
                    self.write_decimal(lat_str.len() as u64).await?;
                    self.stream.write_all(lat_str.as_bytes()).await?;
                    self.stream.write_all(b"\r\n").await?;
                }
                None => {
                    self.stream.write_all(b"*-1\r\n").await?;
                }
            }
        }

        self.stream.flush().await
    }

    pub async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            }
            Frame::Bulk(val) => {
                let len = val.len();

                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Error(msg) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(msg.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Array(parts) => {
                let bytes = Frame::Array(parts.clone()).to_vec();
                self.stream.write_all(&bytes).await?;
            }
        }

        Ok(())
    }

    async fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        use std::io::Write;

        let mut buf = [0u8, 20];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}
