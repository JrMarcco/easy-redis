use crate::resp::{calc_total_len, parse_len, BUF_CAP, CRLF_LEN};
use crate::{RespDecode, RespEncode, RespErr, RespFrame};
use bytes::{Buf, BytesMut};
use std::ops::Deref;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Set(Vec<RespFrame>);

// Sets are somewhat like Arrays but are unordered and should only contain unique elements.
// ~<number-of-elements>\r\n<element-1>...<element-n>
impl RespEncode for Set {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(BUF_CAP);
        buf.extend_from_slice(&format!("~{}\r\n", self.len()).into_bytes());
        for frame in self.0 {
            buf.extend_from_slice(&frame.encode());
        }
        buf
    }
}

impl RespDecode for Set {
    const PREFIX: &'static str = "~";

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
        Ok(Set::new(frames))
    }

    // noinspection DuplicatedCode
    fn expect_len(buf: &[u8]) -> Result<usize, RespErr> {
        let (end, len) = parse_len(buf, Self::PREFIX)?;
        calc_total_len(buf, end, len, Self::PREFIX)
    }
}

impl Set {
    pub fn new(s: impl Into<Vec<RespFrame>>) -> Self {
        Set(s.into())
    }
}

impl From<Vec<RespFrame>> for Set {
    fn from(value: Vec<RespFrame>) -> Self {
        Set(value)
    }
}

impl Deref for Set {
    type Target = Vec<RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Array, BulkString, RespFrame, SimpleString};
    use anyhow::Result;

    #[test]
    fn test_set_encode() {
        let frame: RespFrame = Set::new(vec![
            Array::new(vec![123.into(), true.into()]).into(),
            BulkString::new("hello".to_string()).into(),
        ])
        .into();

        assert_eq!(
            frame.encode(),
            b"~2\r\n*2\r\n:+123\r\n#t\r\n$5\r\nhello\r\n"
        );
    }

    #[test]
    fn test_set_decode() -> Result<()> {
        let set = Set::new(vec![
            Array::new(vec![
                123.into(),
                true.into(),
                SimpleString::new("foo").into(),
            ])
            .into(),
            BulkString::new("hello".to_string()).into(),
        ]);

        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"~2\r\n*3\r\n:+123\r\n#t\r\n+foo\r\n$5\r\nhello\r\n");

        let frame = Set::decode(&mut buf)?;
        assert_eq!(frame, set);

        buf.extend_from_slice(b"~2\r\n*3\r\n:+123\r\n#t\r\n+foo\r\n$5\r\n");

        let ret = Set::decode(&mut buf);
        assert_eq!(ret.unwrap_err(), RespErr::NotComplete);

        buf.extend_from_slice(b"hello\r\n");

        let frame = Set::decode(&mut buf)?;
        assert_eq!(frame, set);
        Ok(())
    }
}
