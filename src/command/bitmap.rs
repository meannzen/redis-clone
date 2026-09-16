
use bytes::Bytes;

use crate::{Connection, Frame, parse::Parse, store::Db};

#[derive(Debug)]
pub struct SetBit {
    pub key: String,
    pub bit_index: usize,
    pub value: u8,
}

#[derive(Debug)]
pub struct  GetBit {
    pub key: String,
    pub bit_index: usize,
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

        let value_set : u8;

        let mut response: u64 = 0 ;

        if value.is_empty() {
            value_set = 0;
        } else {
            value_set = vector[bit_idx];
            response = ((value_set >> bit_pos) & 1) as u64
        }

        vector[bit_idx] = value_set ^ (((self.value > 0) as u8) << bit_pos);
        let value = Bytes::from(vector);
        db.set(self.key, value, None);
        conn.write_frame(&crate::Frame::Integer(response)).await?;
        Ok(())
    }
}


impl GetBit {
   pub fn new(key: String, bit_index: usize) ->Self {
       Self { key, bit_index }
   }

   pub fn key(&self) ->&str {
       &self.key
   }

   pub fn parse_frame(parse: &mut Parse) ->crate::Result<GetBit> {
       let key =  parse.next_string()?;
       let bit_index : usize = parse.next_int()? as usize;

       Ok(Self { key, bit_index })
   }

   pub async  fn apply(self, db: &Db, conn: &mut Connection)->crate::Result<()> {

        let bit_idx = self.bit_index / 8;
        let bit_pos = self.bit_index % 8;

       let response = match db.get(&self.key) {
           Some(value) =>  {
               if let Some(b) = value.get(bit_idx) {
                   ((b >> bit_pos) & 1) as u64
               } else {
                   0
               }
           },
           None => 0
       };

       let frame = Frame::Integer(response);
       conn.write_frame(&frame).await?;

       Ok(())
   }
}
