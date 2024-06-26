use crate::resp::{calc_total_len, parse_len, BUF_CAP, CRLF_LEN};
use crate::{RespDecode, RespEncode, RespErr, RespFrame, SimpleString};
use bytes::{Buf, BytesMut};
use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Map(BTreeMap<String, RespFrame>);

// The RESP map encodes a collection of key-value tuples, i.e., a dictionary or a hash.
// %<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>
//
// !!here only support string key encode with to simple string!!
impl RespEncode for Map {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(BUF_CAP);
        buf.extend_from_slice(&format!("%{}\r\n", self.len()).into_bytes());

        for (key, value) in self.0 {
            buf.extend_from_slice(&SimpleString::new(key).encode());
            buf.extend_from_slice(&value.encode());
        }
        buf
    }
}

impl RespDecode for Map {
    const PREFIX: &'static str = "%";

    fn decode(buf: &mut BytesMut) -> Result<Self, RespErr> {
        let (end, len) = parse_len(buf, Self::PREFIX)?;
        let total_len = calc_total_len(buf, end, len, Self::PREFIX)?;

        if buf.len() < total_len {
            return Err(RespErr::NotComplete);
        }

        buf.advance(end + CRLF_LEN);

        let mut frames = Map::new();
        for _ in 0..len {
            let key = SimpleString::decode(buf)?;
            let value = RespFrame::decode(buf)?;
            frames.insert(key.0, value);
        }

        Ok(frames)
    }

    // noinspection DuplicatedCode
    fn expect_len(buf: &[u8]) -> Result<usize, RespErr> {
        let (end, len) = parse_len(buf, Self::PREFIX)?;
        calc_total_len(buf, end, len, Self::PREFIX)
    }
}

impl Map {
    pub fn new() -> Self {
        Map(BTreeMap::new())
    }
}

impl Default for Map {
    fn default() -> Self {
        Map::new()
    }
}

impl From<BTreeMap<String, RespFrame>> for Map {
    fn from(value: BTreeMap<String, RespFrame>) -> Self {
        Map(value)
    }
}

impl Deref for Map {
    type Target = BTreeMap<String, RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Map {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BulkString, RespFrame};
    use anyhow::Result;

    #[test]
    fn test_map_encode() {
        let mut map = Map::new();
        map.insert(
            "hello".to_string(),
            BulkString::new("world".to_string()).into(),
        );

        map.insert("foo".to_string(), 1.23.into());
        map.insert("int".to_string(), 456.into());

        let frame: RespFrame = map.into();
        assert_eq!(
            frame.encode(),
            b"%3\r\n+foo\r\n,+1.23\r\n+hello\r\n$5\r\nworld\r\n+int\r\n:+456\r\n"
        )
    }

    #[test]
    fn test_map_decode() -> Result<()> {
        let mut map = Map::new();
        map.insert(
            "hello".to_string(),
            BulkString::new(b"world".to_vec()).into(),
        );

        map.insert("foo".to_string(), 1.23.into());
        map.insert("int".to_string(), 456.into());

        let mut buf = BytesMut::new();
        buf.extend_from_slice(
            b"%3\r\n+foo\r\n,+1.23\r\n+hello\r\n$5\r\nworld\r\n+int\r\n:+456\r\n",
        );

        let frame = Map::decode(&mut buf)?;

        assert_eq!(frame, map);

        buf.extend_from_slice(b"%3\r\n+foo\r\n,+1.23\r\n+hello\r\n$5\r\nworld\r\n+int\r\n");
        let ret = Map::decode(&mut buf);
        assert_eq!(ret.unwrap_err(), RespErr::NotComplete);

        Ok(())
    }
}
