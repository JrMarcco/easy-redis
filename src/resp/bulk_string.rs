use crate::resp::{extract_fixed_data, parse_len, CRLF_LEN};
use crate::{RespDecode, RespEncode, RespErr};
use bytes::{Buf, BytesMut};
use std::ops::Deref;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct BulkString(pub(crate) Vec<u8>);

// A bulk string represents a single binary string.
// $<length>\r\n<data>\r\n
impl RespEncode for BulkString {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.len() + 16);
        buf.extend_from_slice(&format!("${}\r\n", self.len()).into_bytes());
        buf.extend_from_slice(&self);
        buf.extend_from_slice(b"\r\n");
        buf
    }
}

impl RespDecode for BulkString {
    const PREFIX: &'static str = "$";

    fn decode(buf: &mut BytesMut) -> Result<Self, RespErr> {
        let (end, len) = parse_len(buf, Self::PREFIX)?;

        let remained = &buf[end + CRLF_LEN..];
        if remained.len() < len + CRLF_LEN {
            return Err(RespErr::NotComplete);
        }

        buf.advance(end + CRLF_LEN);

        let data = buf.split_to(len + CRLF_LEN);
        Ok(BulkString::new(data[..len].to_vec()))
    }

    fn expect_len(buf: &[u8]) -> Result<usize, RespErr> {
        let (end, len) = parse_len(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN + len + CRLF_LEN)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct NullBulkString;

// The null bulk string represents a non-existing value.
// $-1\r\n
impl RespEncode for NullBulkString {
    fn encode(self) -> Vec<u8> {
        b"$-1\r\n".to_vec()
    }
}

impl RespDecode for NullBulkString {
    const PREFIX: &'static str = "$";

    fn decode(buf: &mut BytesMut) -> Result<Self, RespErr> {
        extract_fixed_data(buf, "$-1\r\n", "NullBulkString")?;
        Ok(NullBulkString)
    }

    fn expect_len(_buf: &[u8]) -> Result<usize, RespErr> {
        Ok(5)
    }
}

impl BulkString {
    pub fn new(s: impl Into<Vec<u8>>) -> Self {
        BulkString(s.into())
    }
}

impl From<&str> for BulkString {
    fn from(value: &str) -> Self {
        BulkString(value.as_bytes().to_vec())
    }
}

impl From<String> for BulkString {
    fn from(value: String) -> Self {
        BulkString(value.into_bytes())
    }
}

impl From<&[u8]> for BulkString {
    fn from(value: &[u8]) -> Self {
        BulkString(value.to_vec())
    }
}

impl<const N: usize> From<&[u8; N]> for BulkString {
    fn from(value: &[u8; N]) -> Self {
        BulkString(value.to_vec())
    }
}

impl AsRef<[u8]> for BulkString {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Deref for BulkString {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RespFrame;
    use anyhow::Result;

    #[test]
    fn test_bulk_string_encode() {
        let frame: RespFrame = BulkString::new(b"hello".to_vec()).into();
        assert_eq!(frame.encode(), b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_null_bulk_string_encode() {
        let frame: RespFrame = NullBulkString.into();
        assert_eq!(frame.encode(), b"$-1\r\n");
    }

    #[test]
    fn test_bulk_string_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"$5\r\nhello\r\n");

        let frame = BulkString::decode(&mut buf)?;
        assert_eq!(frame, BulkString::new(b"hello"));

        buf.extend_from_slice(b"$5\r\nhello");
        let ret = BulkString::decode(&mut buf);
        assert_eq!(ret.unwrap_err(), RespErr::NotComplete);

        buf.extend_from_slice(b"\r\n");
        let frame = BulkString::decode(&mut buf)?;
        assert_eq!(frame, BulkString::new(b"hello"));

        Ok(())
    }

    #[test]
    fn test_null_bulk_string_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"$-1\r\n");

        let frame = NullBulkString::decode(&mut buf)?;
        assert_eq!(frame, NullBulkString);

        Ok(())
    }
}
