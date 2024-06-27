// map cmd
use crate::{
    cmd::{extract_args, validate_cmd, CmdErr, CmdExecutor, Get, Set, RESP_OK},
    Array, Backend, Null, RespFrame,
};

// cmd set
impl CmdExecutor for Set {
    fn exec(self, backend: &Backend) -> RespFrame {
        backend.set(self.key, self.value);
        RESP_OK.clone()
    }
}

impl TryFrom<Array> for Set {
    type Error = CmdErr;

    fn try_from(value: Array) -> Result<Self, Self::Error> {
        validate_cmd(&value, &["set"], 2)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(value)) => Ok(Set {
                key: String::from_utf8(key.0)?,
                value,
            }),
            _ => Err(CmdErr::InvalidArg("Invalid key or value.".to_string())),
        }
    }
}

// cmd get
impl CmdExecutor for Get {
    fn exec(self, backend: &Backend) -> RespFrame {
        backend.get(&self.key).unwrap_or(RespFrame::Null(Null))
    }
}

impl TryFrom<Array> for Get {
    type Error = CmdErr;

    fn try_from(value: Array) -> Result<Self, Self::Error> {
        validate_cmd(&value, &["get"], 1)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(Get {
                key: String::from_utf8(key.0)?,
            }),
            _ => Err(CmdErr::InvalidArg("Invalid key.".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RespDecode;
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_set_from_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n");

        let frame = Array::decode(&mut buf)?;

        let cmd: Set = frame.try_into()?;
        assert_eq!(cmd.key, "hello");
        assert_eq!(cmd.value, RespFrame::BulkString("world".into()));

        Ok(())
    }

    #[test]
    fn test_get_from_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$3\r\nget\r\n$5\r\nhello\r\n");

        let frame = Array::decode(&mut buf)?;

        let cmd: Get = frame.try_into()?;
        assert_eq!(cmd.key, "hello");

        Ok(())
    }

    #[test]
    fn test_map_cmd() -> Result<()> {
        let backend = Backend::new();

        let cmd = Set {
            key: "hello".to_string(),
            value: RespFrame::BulkString("world".into()),
        };

        let ret = cmd.exec(&backend);
        assert_eq!(ret, RESP_OK.clone());

        let cmd = Get {
            key: "hello".to_string(),
        };

        let ret = cmd.exec(&backend);
        assert_eq!(ret, RespFrame::BulkString("world".into()));

        Ok(())
    }
}
