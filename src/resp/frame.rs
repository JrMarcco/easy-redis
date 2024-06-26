use bytes::BytesMut;
use enum_dispatch::enum_dispatch;

use crate::{
    Array, BulkString, Map, Null, NullArray, NullBulkString, RespDecode, RespErr, Set, SimpleError,
    SimpleString,
};

#[enum_dispatch(RespEncode)]
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum RespFrame {
    SimpleString(SimpleString),
    SimpleError(SimpleError),
    Integer(i64),
    BulkString(BulkString),
    NullBulkString(NullBulkString),
    Array(Array),
    NullArray(NullArray),
    Null(Null),
    Boolean(bool),
    Double(f64),
    Map(Map),
    Set(Set),
}

impl RespDecode for RespFrame {
    const PREFIX: &'static str = "";

    fn decode(buf: &mut BytesMut) -> Result<Self, RespErr> {
        let mut iter = buf.iter().peekable();
        match iter.next() {
            Some(b'+') => {
                let frame = SimpleString::decode(buf)?;
                Ok(frame.into())
            }
            Some(b'-') => {
                let frame = SimpleError::decode(buf)?;
                Ok(frame.into())
            }
            Some(b':') => {
                let frame = i64::decode(buf)?;
                Ok(frame.into())
            }
            Some(b'$') => {
                // try null first
                match NullBulkString::decode(buf) {
                    Ok(frame) => Ok(frame.into()),
                    Err(RespErr::NotComplete) => Err(RespErr::NotComplete),
                    Err(_) => {
                        let frame = BulkString::decode(buf)?;
                        Ok(frame.into())
                    }
                }
            }
            Some(b'*') => {
                // try null first
                match NullArray::decode(buf) {
                    Ok(frame) => Ok(frame.into()),
                    Err(RespErr::NotComplete) => Err(RespErr::NotComplete),
                    Err(_) => {
                        let frame = Array::decode(buf)?;
                        Ok(frame.into())
                    }
                }
            }
            Some(b'_') => {
                let frame = Null::decode(buf)?;
                Ok(frame.into())
            }
            Some(b'#') => {
                let frame = bool::decode(buf)?;
                Ok(frame.into())
            }
            Some(b',') => {
                let frame = f64::decode(buf)?;
                Ok(frame.into())
            }
            Some(b'%') => {
                let frame = Map::decode(buf)?;
                Ok(frame.into())
            }
            Some(b'~') => {
                let frame = Set::decode(buf)?;
                Ok(frame.into())
            }
            _ => Err(RespErr::NotComplete),
        }
    }

    fn expect_len(buf: &[u8]) -> Result<usize, RespErr> {
        let mut iter = buf.iter().peekable();
        match iter.next() {
            Some(b'+') => SimpleString::expect_len(buf),
            Some(b'-') => SimpleError::expect_len(buf),
            Some(b':') => i64::expect_len(buf),
            Some(b'$') => BulkString::expect_len(buf),
            Some(b'*') => Array::expect_len(buf),
            Some(b'_') => Null::expect_len(buf),
            Some(b'#') => bool::expect_len(buf),
            Some(b',') => f64::expect_len(buf),
            Some(b'%') => Map::expect_len(buf),
            Some(b'~') => Set::expect_len(buf),
            _ => Err(RespErr::NotComplete),
        }
    }
}

impl<const N: usize> From<&[u8; N]> for RespFrame {
    fn from(value: &[u8; N]) -> Self {
        BulkString(value.to_vec()).into()
    }
}
