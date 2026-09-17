use bytes::Bytes;

use crate::{parse::Parse, store::Db, Connection, Frame};

#[derive(Debug)]
pub struct SetBit {
    pub key: String,
    pub bit_index: usize,
    pub value: u8,
}

#[derive(Debug)]
pub struct GetBit {
    pub key: String,
    pub bit_index: usize,
}

#[derive(Debug)]
pub struct STRLEN {
    pub key: String,
}

#[derive(Debug)]
pub struct BitCount {
    pub key: String,
    pub range: Range,
}

#[derive(Debug)]
pub struct Range {
    pub start: Option<i32>,
    pub end: Option<i32>,
}

impl SetBit {
    pub fn new(key: String, bit_index: usize, value: u8) -> Self {
        Self {
            key,
            bit_index,
            value,
        }
    }

    #[inline]
    pub fn key(&self) -> &str {
        self.key.as_str()
    }

    pub fn parse_frame(parse: &mut Parse) -> crate::Result<SetBit> {
        let key = parse.next_string()?;
        let index = parse.next_int()?;
        let value = parse.next_int()?;

        Ok(Self {
            key,
            bit_index: index as usize,
            value: value as u8,
        })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let value = match db.get(self.key()) {
            Some(bytes) => bytes.clone(),
            None => Bytes::new(),
        };

        let bit_idx = self.bit_index / 8;
        let bit_pos = self.bit_index % 8;

        let mut vector = value.to_vec();
        if vector.len() <= bit_idx {
            vector.resize(bit_idx + 1, 0);
        }

        let old_byte = vector[bit_idx];
        let response = ((old_byte >> (7 - bit_pos)) & 1) as u64;

        let mask = 1u8 << (7 - bit_pos);
        if self.value > 0 {
            vector[bit_idx] = old_byte | mask;
        } else {
            vector[bit_idx] = old_byte & !mask;
        }

        let value = Bytes::from(vector);
        db.set(self.key, value, None);

        conn.write_frame(&crate::Frame::Integer(response)).await?;
        Ok(())
    }
}

impl GetBit {
    pub fn new(key: String, bit_index: usize) -> Self {
        Self { key, bit_index }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn parse_frame(parse: &mut Parse) -> crate::Result<GetBit> {
        let key = parse.next_string()?;
        let bit_index: usize = parse.next_int()? as usize;

        Ok(Self { key, bit_index })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let bit_idx = self.bit_index / 8;
        let bit_pos = self.bit_index % 8;

        let response = match db.get(&self.key) {
            Some(value) => {
                if let Some(b) = value.get(bit_idx) {
                    ((b >> (7 - bit_pos)) & 1) as u64
                } else {
                    0
                }
            }
            None => 0,
        };

        let frame = Frame::Integer(response);
        conn.write_frame(&frame).await?;

        Ok(())
    }
}

impl STRLEN {
    pub fn new(key: impl ToString) -> Self {
        Self {
            key: key.to_string(),
        }
    }

    pub fn parse_frame(parse: &mut Parse) -> crate::Result<STRLEN> {
        Ok(Self {
            key: parse.next_string()?,
        })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let response = match db.get(&self.key) {
            Some(value) => value.len() as u64,
            None => 0,
        };

        conn.write_frame(&Frame::Integer(response)).await?;

        Ok(())
    }
}

impl BitCount {
    pub fn new(key: String, range: Range) -> Self {
        Self { key, range }
    }

    #[inline]
    pub fn start(&self) -> Option<i32> {
        self.range.start
    }

    #[inline]
    pub fn end(&self) -> Option<i32> {
        self.range.end
    }

    pub fn parse_frame(parse: &mut Parse) -> crate::Result<BitCount> {
        let key = parse.next_string()?;
        let start: Option<i32> = parse.next_string().ok().and_then(|s| s.parse().ok());
        let end: Option<i32> = parse.next_string().ok().and_then(|s| s.parse().ok());
        Ok(Self {
            key,
            range: Range { start, end },
        })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let response = match db.get(&self.key) {
            Some(value) => {
                let bytes = value.as_ref();
                let len = bytes.len() as i32;

                let mut start = self.start().unwrap_or(0);
                if start < 0 {
                    start = len + start;
                }
                start = start.max(0);

                let mut end = self.end().unwrap_or(len - 1);
                if end < 0 {
                    end = len + end;
                }
                end = end.min(len - 1);

                if start > end || len == 0 {
                    0
                } else {
                    let mut count = 0u64;
                    for &byte in &bytes[start as usize..=end as usize] {
                        count += count_one(byte) as u64;
                    }
                    count
                }
            }
            None => 0,
        };

        conn.write_frame(&Frame::Integer(response)).await?;
        Ok(())
    }
}

fn count_one(mut n: u8) -> u32 {
    let mut count = 0;
    while n != 0 {
        n = n & (n - 1);
        count += 1;
    }
    count
}
