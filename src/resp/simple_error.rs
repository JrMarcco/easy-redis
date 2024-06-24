use crate::resp::{extract_frame_data, CRLF_LEN};
use crate::{RespDecode, RespEncode, RespErr};
use bytes::BytesMut;
use std::ops::Deref;

// -Error message\r\n
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct SimpleError(pub(crate) String);

impl RespEncode for SimpleError {
    fn encode(self) -> Vec<u8> {
        format!("-{}\r\n", self.0).into_bytes()
    }
}

impl RespDecode for SimpleError {
    const PREFIX: &'static str = "-";

    fn decode(buf: &mut BytesMut) -> Result<Self, RespErr> {
        let end = extract_frame_data(buf, Self::PREFIX)?;
        let data = buf.split_to(end + CRLF_LEN);

        let s = String::from_utf8_lossy(&data[Self::PREFIX.len()..end]);
        Ok(SimpleError::new(s.to_string()))
    }

    fn expect_len(buf: &[u8]) -> Result<usize, RespErr> {
        let end = extract_frame_data(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN)
    }
}

impl SimpleError {
    fn new(s: impl Into<String>) -> Self {
        SimpleError(s.into())
    }
}

impl From<&str> for SimpleError {
    fn from(value: &str) -> Self {
        SimpleError(value.to_string())
    }
}

impl Deref for SimpleError {
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

    #[test]
    fn test_simple_error_encode() {
        let frame: RespFrame = SimpleError::new("Error message".to_string()).into();
        assert_eq!(frame.encode(), b"-Error message\r\n");
    }

    #[test]
    fn test_simple_error_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"-Error message\r\n");

        let frame = SimpleError::decode(&mut buf)?;
        assert_eq!(frame, SimpleError::new("Error message".to_string()));

        Ok(())
    }
}
