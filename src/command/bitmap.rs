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
