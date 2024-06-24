use bytes::{Buf, BytesMut};
use enum_dispatch::enum_dispatch;
use thiserror::Error;

pub use self::{
    bulk_string::BulkString, bulk_string::NullBulkString, frame::RespFrame,
    simple_error::SimpleError, simple_string::SimpleString,
};

mod bulk_string;
mod frame;
mod integer;
mod simple_error;
mod simple_string;

const CRLF: &[u8] = b"\r\n";
const CRLF_LEN: usize = CRLF.len();

#[enum_dispatch]
pub trait RespEncode {
    fn encode(self) -> Vec<u8>;
}

pub trait RespDecode: Sized {
    const PREFIX: &'static str;

    fn decode(buf: &mut BytesMut) -> Result<Self, RespErr>;
    fn expect_len(buf: &[u8]) -> Result<usize, RespErr>;
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RespErr {
    #[error("Frame is not complete.")]
    NotComplete,
    #[error("Invalid frame type: {0}")]
    InvalidFrameType(String),

    #[error("Parse error: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),
}

fn find_crlf(buf: &[u8], nth: usize) -> Option<usize> {
    let mut count = 0;
    for i in 1..buf.len() - 1 {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            count += 1;
            if count == nth {
                return Some(i);
            }
        }
    }

    None
}

fn extract_frame_data(buf: &[u8], prefix: &str) -> Result<usize, RespErr> {
    if buf.len() < 3 {
        return Err(RespErr::NotComplete);
    }

    if !buf.starts_with(prefix.as_bytes()) {
        return Err(RespErr::InvalidFrameType(format!(
            "expect: {}, got: {:?}",
            prefix, buf
        )));
    }

    let end = find_crlf(buf, 1).ok_or(RespErr::NotComplete)?;
    Ok(end)
}

fn extract_fixed_data(buf: &mut BytesMut, expect: &str, expect_type: &str) -> Result<(), RespErr> {
    if buf.len() < expect.len() {
        return Err(RespErr::NotComplete);
    }

    if !buf.starts_with(expect.as_bytes()) {
        return Err(RespErr::InvalidFrameType(format!(
            "expect: {}, got: {:?}",
            expect_type, buf
        )));
    }

    buf.advance(expect.len());
    Ok(())
}

fn parse_len(buf: &[u8], prefix: &str) -> Result<(usize, usize), RespErr> {
    let end = extract_frame_data(buf, prefix)?;
    let s = String::from_utf8_lossy(&buf[prefix.len()..end]);
    Ok((end, s.parse()?))
}
