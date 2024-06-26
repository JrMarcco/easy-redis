use crate::{RespFrame, SimpleString};
use lazy_static::lazy_static;
use thiserror::Error;

lazy_static! {
    static ref RESP_OK: RespFrame = SimpleString::new("Ok").into();
}

pub trait CmdExecutor {
    fn exec() -> RespFrame;
}

#[derive(Debug, Error)]
pub enum CmdErr {}
