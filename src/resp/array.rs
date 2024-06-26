use crate::resp::{calc_total_len, extract_fixed_data, parse_len, BUF_CAP, CRLF_LEN};
use crate::{RespDecode, RespEncode, RespErr, RespFrame};
use bytes::{Buf, BytesMut};
use std::ops::Deref;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Array(pub(crate) Vec<RespFrame>);

// Clients send commands to the Redis server as RESP arrays.
// Similarly, some Redis commands that return collections of elements use arrays as their replies.
// *<number-of-elements>\r\n<element-1>...<element-n>
impl RespEncode for Array {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(BUF_CAP);
        buf.extend_from_slice(&format!("*{}\r\n", self.0.len()).into_bytes());
        for frame in self.0 {
            buf.extend_from_slice(&frame.encode());
        }
        buf
    }
}

impl RespDecode for Array {
    const PREFIX: &'static str = "*";

    fn decode(buf: &mut BytesMut) -> Result<Self, RespErr> {
        let (end, len) = parse_len(buf, Self::PREFIX)?;
        let total_len = calc_total_len(buf, end, len, Self::PREFIX)?;

        if buf.len() < total_len {
            return Err(RespErr::NotComplete);
        }

        buf.advance(end + CRLF_LEN);

        let mut frames = Vec::with_capacity(len);
        for _ in 0..len {
            frames.push(RespFrame::decode(buf)?);
        }

        Ok(Array::new(frames))
    }

    // noinspection DuplicatedCode
    fn expect_len(buf: &[u8]) -> Result<usize, RespErr> {
        let (end, len) = parse_len(buf, Self::PREFIX)?;
        calc_total_len(buf, end, len, Self::PREFIX)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct NullArray;

// Null arrays exist as an alternative way of representing a null value.
// *-1\r\n
impl RespEncode for NullArray {
    fn encode(self) -> Vec<u8> {
        b"*-1\r\n".to_vec()
    }
}

impl RespDecode for NullArray {
    const PREFIX: &'static str = "*";

    fn decode(buf: &mut BytesMut) -> Result<Self, RespErr> {
        extract_fixed_data(buf, "*-1\r\n", "NullArray")?;
        Ok(NullArray)
    }

    fn expect_len(_buf: &[u8]) -> Result<usize, RespErr> {
        Ok(4)
    }
}

impl Array {
    pub fn new(s: impl Into<Vec<RespFrame>>) -> Self {
        Array(s.into())
    }
}

impl From<Vec<RespFrame>> for Array {
    fn from(value: Vec<RespFrame>) -> Self {
        Array(value)
    }
}

impl Deref for Array {
    type Target = Vec<RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BulkString, RespFrame};
    use anyhow::Result;

    #[test]
    fn test_array_encode() {
        let frame: RespFrame = Array::new(vec![
            BulkString::new("set".to_string()).into(),
            BulkString::new("hello".to_string()).into(),
            BulkString::new("world".to_string()).into(),
        ])
        .into();

        assert_eq!(
            &frame.encode(),
            b"*3\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
        );
    }

    #[test]
    fn test_null_array_encode() {
        let frame: RespFrame = NullArray.into();
        assert_eq!(frame.encode(), b"*-1\r\n");
    }

    #[test]
    fn test_array_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n");

        let frame = Array::decode(&mut buf)?;
        assert_eq!(
            frame,
            Array::new([b"set".into(), b"hello".into(), b"world".into()])
        );

        buf.extend_from_slice(b"*3\r\n$3\r\nset\r\n$5\r\nhello\r\n");
        let ret = Array::decode(&mut buf);
        assert_eq!(ret.unwrap_err(), RespErr::NotComplete);

        buf.extend_from_slice(b"$5\r\nworld\r\n");
        let frame = Array::decode(&mut buf)?;
        assert_eq!(
            frame,
            Array::new([b"set".into(), b"hello".into(), b"world".into()])
        );

        buf.extend_from_slice(b"*3\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\n");
        let ret = Array::decode(&mut buf);
        assert_eq!(ret.unwrap_err(), RespErr::NotComplete);

        buf.extend_from_slice(b"world\r\n");
        let frame = Array::decode(&mut buf)?;
        assert_eq!(
            frame,
            Array::new([b"set".into(), b"hello".into(), b"world".into()])
        );

        Ok(())
    }

    #[test]
    fn test_null_array_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*-1\r\n");

        let frame = NullArray::decode(&mut buf)?;
        assert_eq!(frame, NullArray);

        Ok(())
    }
}
