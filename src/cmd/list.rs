use crate::cmd::{
    extract_args, validate_cmd, CmdErr, CmdExecutor, LLen, LPop, LPush, RPop, RPush, RESP_OK,
};
use crate::{Array, Backend, Null, RespFrame};

impl CmdExecutor for LPush {
    fn exec(self, backend: &Backend) -> RespFrame {
        backend.lpush(self.key, self.value);
        RESP_OK.clone()
    }
}

impl TryFrom<Array> for LPush {
    type Error = CmdErr;

    fn try_from(value: Array) -> Result<Self, Self::Error> {
        validate_cmd(&value, &["lpush"], 2)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(value)) => Ok(LPush {
                key: String::from_utf8(key.0)?,
                value,
            }),
            _ => Err(CmdErr::InvalidArg("Invalid key or value".to_string())),
        }
    }
}

impl CmdExecutor for LPop {
    fn exec(self, backend: &Backend) -> RespFrame {
        backend.lpop(&self.key).unwrap_or(RespFrame::Null(Null))
    }
}

impl TryFrom<Array> for LPop {
    type Error = CmdErr;

    fn try_from(value: Array) -> Result<Self, Self::Error> {
        validate_cmd(&value, &["lpop"], 1)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(LPop {
                key: String::from_utf8(key.0)?,
            }),
            _ => Err(CmdErr::InvalidArg("Invalid key".to_string())),
        }
    }
}

impl CmdExecutor for RPush {
    fn exec(self, backend: &Backend) -> RespFrame {
        backend.rpush(self.key, self.value);
        RESP_OK.clone()
    }
}

impl TryFrom<Array> for RPush {
    type Error = CmdErr;

    fn try_from(value: Array) -> Result<Self, Self::Error> {
        validate_cmd(&value, &["rpush"], 2)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(value)) => Ok(RPush {
                key: String::from_utf8(key.0)?,
                value,
            }),
            _ => Err(CmdErr::InvalidArg("Invalid key or value".to_string())),
        }
    }
}

impl CmdExecutor for RPop {
    fn exec(self, backend: &Backend) -> RespFrame {
        backend.rpop(&self.key).unwrap_or(RespFrame::Null(Null))
    }
}

impl TryFrom<Array> for RPop {
    type Error = CmdErr;

    fn try_from(value: Array) -> Result<Self, Self::Error> {
        validate_cmd(&value, &["rpop"], 1)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(RPop {
                key: String::from_utf8(key.0)?,
            }),
            _ => Err(CmdErr::InvalidArg("Invalid key".to_string())),
        }
    }
}

impl CmdExecutor for LLen {
    fn exec(self, backend: &Backend) -> RespFrame {
        match backend.llen(&self.key) {
            Some(len) => RespFrame::Integer(len as i64),
            _ => RespFrame::Integer(0),
        }
    }
}

impl TryFrom<Array> for LLen {
    type Error = CmdErr;

    fn try_from(value: Array) -> Result<Self, Self::Error> {
        validate_cmd(&value, &["llen"], 1)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(LLen {
                key: String::from_utf8(key.0)?,
            }),
            _ => Err(CmdErr::InvalidArg("Invalid key".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use bytes::BytesMut;

    use crate::RespDecode;

    use super::*;

    #[test]
    fn test_lpush_from_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$5\r\nlpush\r\n$8\r\njrmarcco\r\n$5\r\nhello\r\n");

        let frame = Array::decode(&mut buf)?;

        let cmd: LPush = frame.try_into()?;
        assert_eq!(cmd.key, "jrmarcco");
        assert_eq!(cmd.value, RespFrame::BulkString("hello".into()));

        Ok(())
    }

    #[test]
    fn test_lpop_from_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$4\r\nlpop\r\n$8\r\njrmarcco\r\n");

        let frame = Array::decode(&mut buf)?;

        let cmd: LPop = frame.try_into()?;
        assert_eq!(cmd.key, "jrmarcco");

        Ok(())
    }

    #[test]
    fn test_rpush_from_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$5\r\nrpush\r\n$8\r\njrmarcco\r\n$5\r\nhello\r\n");

        let frame = Array::decode(&mut buf)?;

        let cmd: RPush = frame.try_into()?;
        assert_eq!(cmd.key, "jrmarcco");
        assert_eq!(cmd.value, RespFrame::BulkString("hello".into()));

        Ok(())
    }

    #[test]
    fn test_rpop_from_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$4\r\nrpop\r\n$8\r\njrmarcco\r\n");

        let frame = Array::decode(&mut buf)?;

        let cmd: RPop = frame.try_into()?;
        assert_eq!(cmd.key, "jrmarcco");

        Ok(())
    }

    #[test]
    fn test_llen_from_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$4\r\nllen\r\n$8\r\njrmarcco\r\n");

        let frame = Array::decode(&mut buf)?;
        let cmd: LLen = frame.try_into()?;
        assert_eq!(cmd.key, "jrmarcco");

        Ok(())
    }

    #[test]
    fn test_list_cmd() -> Result<()> {
        let backend = Backend::new();

        let cmd = LLen {
            key: "jrmarcco".to_string(),
        };
        let ret = cmd.exec(&backend);
        assert_eq!(ret, RespFrame::Integer(0));

        let cmd = LPush {
            key: "jrmarcco".to_string(),
            value: RespFrame::BulkString("hello".into()),
        };

        let ret = cmd.exec(&backend);
        assert_eq!(ret, RESP_OK.clone());

        let cmd = LPush {
            key: "jrmarcco".to_string(),
            value: RespFrame::BulkString("world".into()),
        };

        let ret = cmd.exec(&backend);
        assert_eq!(ret, RESP_OK.clone());

        let cmd = LPop {
            key: "jrmarcco".to_string(),
        };

        let ret = cmd.exec(&backend);
        assert_eq!(ret, RespFrame::BulkString("world".into()));

        let cmd = RPush {
            key: "jrmarcco".to_string(),
            value: RespFrame::Double(1.23),
        };
        let ret = cmd.exec(&backend);
        assert_eq!(ret, RESP_OK.clone());

        let cmd = RPush {
            key: "jrmarcco".to_string(),
            value: RespFrame::Double(4.56),
        };
        let ret = cmd.exec(&backend);
        assert_eq!(ret, RESP_OK.clone());

        let cmd = RPop {
            key: "jrmarcco".to_string(),
        };
        let ret = cmd.exec(&backend);
        assert_eq!(ret, RespFrame::Double(4.56));

        let cmd = LLen {
            key: "jrmarcco".to_string(),
        };
        let ret = cmd.exec(&backend);
        assert_eq!(ret, RespFrame::Integer(2));

        Ok(())
    }
}
