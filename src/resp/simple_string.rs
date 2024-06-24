use crate::resp::{extract_frame_data, CRLF_LEN};
use crate::{RespDecode, RespEncode, RespErr};
use bytes::BytesMut;
use std::ops::Deref;

// Simple strings are encoded as a plus (+) character, followed by a string.
// The string mustn't contain a CR (\r) or LF (\n) character and is terminated by CRLF (i.e., \r\n).
// +OK\r\n
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct SimpleString(pub(crate) String);

impl RespEncode for SimpleString {
    fn encode(self) -> Vec<u8> {
        format!("+{}\r\n", self.0).into_bytes()
    }
}

impl RespDecode for SimpleString {
    const PREFIX: &'static str = "+";

    fn decode(buf: &mut BytesMut) -> Result<Self, RespErr> {
        let end = extract_frame_data(buf, Self::PREFIX)?;
        let data = buf.split_to(end + CRLF_LEN);

        let s = String::from_utf8_lossy(&data[Self::PREFIX.len()..end]);
        Ok(SimpleString::new(s.to_string()))
    }

    fn expect_len(buf: &[u8]) -> Result<usize, RespErr> {
        let end = extract_frame_data(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN)
    }
}

impl SimpleString {
    pub fn new(s: impl Into<String>) -> Self {
        SimpleString(s.into())
    }
}

impl From<&str> for SimpleString {
    fn from(value: &str) -> Self {
        SimpleString(value.to_string())
    }
}

impl AsRef<str> for SimpleString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Deref for SimpleString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RespFrame;
    use anyhow::Result;
    use bytes::BufMut;

    #[test]
    fn test_simple_string_encode() {
        let frame: RespFrame = SimpleString::new("OK".to_string()).into();
        assert_eq!(frame.encode(), b"+OK\r\n");
    }

    #[test]
    fn test_simple_string_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"+OK\r\n");

        let frame = SimpleString::decode(&mut buf)?;
        assert_eq!(frame, SimpleString::new("OK".to_string()));

        buf.extend_from_slice(b"+hello\r");

        let ret = SimpleString::decode(&mut buf);
        assert_eq!(ret.unwrap_err(), RespErr::NotComplete);

        buf.put_u8(b'\n');
        let frame = SimpleString::decode(&mut buf)?;
        assert_eq!(frame, SimpleString::new("hello".to_string()));

        Ok(())
    }
}
