use bytes::BytesMut;
use enum_dispatch::enum_dispatch;

use crate::{RespDecode, RespErr, SimpleString};

#[enum_dispatch(RespEncode)]
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum RespFrame {
    SimpleString(SimpleString),
}

impl RespDecode for RespFrame {
    const PREFIX: &'static str = "+";

    fn decode(buf: &mut BytesMut) -> Result<Self, RespErr> {
        let mut iter = buf.iter().peekable();
        match iter.next() {
            Some(b'+') => {
                let frame = SimpleString::decode(buf)?;
                Ok(frame.into())
            }
            _ => Err(RespErr::NotComplete),
        }
    }

    fn expect_len(buf: &[u8]) -> Result<usize, RespErr> {
        let mut iter = buf.iter().peekable();
        match iter.next() {
            Some(b'+') => SimpleString::expect_len(buf),
            _ => Err(RespErr::NotComplete),
        }
    }
}
