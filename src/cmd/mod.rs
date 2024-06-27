mod hash_map;
mod map;

use crate::{Array, Backend, RespErr, RespFrame, SimpleString};
use enum_dispatch::enum_dispatch;
use lazy_static::lazy_static;
use thiserror::Error;

lazy_static! {
    static ref RESP_OK: RespFrame = SimpleString::new("Ok").into();
}

#[enum_dispatch]
pub trait CmdExecutor {
    fn exec(self, backend: &Backend) -> RespFrame;
}

#[enum_dispatch(CmdExecutor)]
#[derive(Debug)]
pub enum Cmd {
    Set(Set),
    Get(Get),

    // unrecognized command
    Unrecognized(Unrecognized),
}

#[derive(Debug)]
pub struct Set {
    key: String,
    value: RespFrame,
}

#[derive(Debug)]
pub struct Get {
    key: String,
}

#[derive(Debug)]
pub struct Unrecognized;

impl TryFrom<RespFrame> for Cmd {
    type Error = CmdErr;

    fn try_from(value: RespFrame) -> Result<Self, Self::Error> {
        match value {
            RespFrame::Array(array) => array.try_into(),
            _ => Err(CmdErr::InvalidCmd("Command must be an Array".to_string())),
        }
    }
}

impl TryFrom<Array> for Cmd {
    type Error = CmdErr;

    fn try_from(value: Array) -> Result<Self, Self::Error> {
        match value.first() {
            Some(RespFrame::BulkString(ref cmd)) => match cmd.as_ref() {
                b"set" => Ok(Set::try_from(value)?.into()),
                b"get" => Ok(Get::try_from(value)?.into()),
                _ => Ok(Unrecognized.into()),
            },
            _ => Err(CmdErr::InvalidCmd(
                "Command must have a BulkString as the first argument.".to_string(),
            )),
        }
    }
}

impl CmdExecutor for Unrecognized {
    fn exec(self, _backend: &Backend) -> RespFrame {
        RESP_OK.clone()
    }
}

#[derive(Debug, Error)]
pub enum CmdErr {
    #[error("Invalid command: {0}")]
    InvalidCmd(String),
    #[error("Invalid arguments: {0}")]
    InvalidArg(String),

    #[error("{0}")]
    RespErr(#[from] RespErr),
    #[error("From utf-8 error: {0}")]
    FromUtf8Err(#[from] std::string::FromUtf8Error),
}

fn validate_cmd(value: &Array, names: &[&'static str], args_num: usize) -> Result<(), CmdErr> {
    if value.len() != args_num + names.len() {
        return Err(CmdErr::InvalidArg(format!(
            "{} command must have exactly {} argument.",
            names.join(" "),
            args_num
        )));
    }

    for (i, name) in names.iter().enumerate() {
        match value[i] {
            RespFrame::BulkString(ref cmd) => {
                let cmd_lower = String::from_utf8_lossy(cmd.as_ref()).to_ascii_lowercase();
                if &cmd_lower != name {
                    return Err(CmdErr::InvalidCmd(format!(
                        "Expected {}, got {}",
                        name,
                        String::from_utf8_lossy(cmd.as_ref())
                    )));
                }
            }
            _ => {
                return Err(CmdErr::InvalidCmd(format!(
                    "Command at position {} must be a BulkString",
                    i + 1
                )))
            }
        }
    }

    Ok(())
}

fn extract_args(value: Array, start: usize) -> Result<Vec<RespFrame>, CmdErr> {
    Ok(value.0.into_iter().skip(start).collect::<Vec<RespFrame>>())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Null, RespDecode};
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_cmd() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$3\r\nget\r\n$5\r\nhello\r\n");

        let frame = Array::decode(&mut buf)?;

        let cmd: Cmd = frame.try_into()?;

        let backend = Backend::new();

        let ret = cmd.exec(&backend);
        assert_eq!(ret, RespFrame::Null(Null));
        Ok(())
    }
}
