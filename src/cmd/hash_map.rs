// hash map cmd

use crate::cmd::{extract_args, validate_cmd, CmdErr, CmdExecutor, HGet, HGetAll, HSet, RESP_OK};
use crate::{Array, Backend, BulkString, Null, RespFrame};

// cmd hset
impl CmdExecutor for HSet {
    fn exec(self, backend: &Backend) -> RespFrame {
        backend.hset(self.key, self.field, self.value);
        RESP_OK.clone()
    }
}

impl TryFrom<Array> for HSet {
    type Error = CmdErr;

    fn try_from(value: Array) -> Result<Self, Self::Error> {
        validate_cmd(&value, &["hset"], 3)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match (args.next(), args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(RespFrame::BulkString(field)), Some(value)) => {
                Ok(HSet {
                    key: String::from_utf8(key.0)?,
                    field: String::from_utf8(field.0)?,
                    value,
                })
            }
            _ => Err(CmdErr::InvalidArg(
                "Invalid key, field or value.".to_string(),
            )),
        }
    }
}

// cmd hget
impl CmdExecutor for HGet {
    fn exec(self, backend: &Backend) -> RespFrame {
        backend
            .hget(&self.key, &self.field)
            .unwrap_or(RespFrame::Null(Null))
    }
}

impl TryFrom<Array> for HGet {
    type Error = CmdErr;

    fn try_from(value: Array) -> Result<Self, Self::Error> {
        validate_cmd(&value, &["hget"], 2)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(RespFrame::BulkString(field))) => Ok(HGet {
                key: String::from_utf8(key.0)?,
                field: String::from_utf8(field.0)?,
            }),
            _ => Err(CmdErr::InvalidArg("Invalid key or field.".to_string())),
        }
    }
}

// cmd hgetall
impl CmdExecutor for HGetAll {
    fn exec(self, backend: &Backend) -> RespFrame {
        let map = backend.hash_map.get(&self.key);

        match map {
            Some(map) => {
                let mut data = Vec::with_capacity(map.len());
                for item in map.iter() {
                    let key = item.key().to_owned();
                    data.push((key, item.value().clone()));
                }

                if self.sort {
                    data.sort_by(|a, b| a.0.cmp(&b.0));
                }

                let ret = data
                    .into_iter()
                    .flat_map(|(k, v)| vec![BulkString::from(k).into(), v])
                    .collect::<Vec<RespFrame>>();

                Array::new(ret).into()
            }
            None => Array::new([]).into(),
        }
    }
}

impl TryFrom<Array> for HGetAll {
    type Error = CmdErr;

    fn try_from(value: Array) -> Result<Self, Self::Error> {
        validate_cmd(&value, &["hgetall"], 1)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(HGetAll {
                key: String::from_utf8(key.0)?,
                sort: false,
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
    fn test_hset_from_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(
            b"*4\r\n$4\r\nhset\r\n$8\r\njrmarcco\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
        );

        let frame = Array::decode(&mut buf)?;

        let cmd: HSet = frame.try_into()?;
        assert_eq!(cmd.key, "jrmarcco");
        assert_eq!(cmd.field, "hello");
        assert_eq!(cmd.value, RespFrame::BulkString("world".into()));

        Ok(())
    }

    #[test]
    fn test_hget_from_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$4\r\nhget\r\n$8\r\njrmarcco\r\n$5\r\nhello\r\n");

        let frame = Array::decode(&mut buf)?;

        let cmd: HGet = frame.try_into()?;
        assert_eq!(cmd.key, "jrmarcco");
        assert_eq!(cmd.field, "hello");

        Ok(())
    }

    #[test]
    fn test_hgetall_from_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$7\r\nhgetall\r\n$8\r\njrmarcco\r\n");

        let frame = Array::decode(&mut buf)?;

        let cmd: HGetAll = frame.try_into()?;
        assert_eq!(cmd.key, "jrmarcco");
        assert!(!cmd.sort);

        Ok(())
    }

    #[test]
    fn test_hash_map_cmd() -> Result<()> {
        let backend = Backend::new();

        let cmd = HSet {
            key: "jrmarcco".to_string(),
            field: "hello".to_string(),
            value: RespFrame::BulkString("world".into()),
        };

        let ret = cmd.exec(&backend);
        assert_eq!(ret, RESP_OK.clone());

        let cmd = HGet {
            key: "jrmarcco".to_string(),
            field: "hello".to_string(),
        };

        let ret = cmd.exec(&backend);
        assert_eq!(ret, RespFrame::BulkString("world".into()));

        let cmd = HSet {
            key: "jrmarcco".to_string(),
            field: "foo".to_string(),
            value: RespFrame::BulkString("bar".into()),
        };

        let ret = cmd.exec(&backend);
        assert_eq!(ret, RESP_OK.clone());

        let cmd = HGet {
            key: "jrmarcco".to_string(),
            field: "foo".to_string(),
        };

        let ret = cmd.exec(&backend);
        assert_eq!(ret, RespFrame::BulkString("bar".into()));

        let cmd: HGetAll = HGetAll {
            key: "jrmarcco".to_string(),
            sort: true,
        };

        let ret = cmd.exec(&backend);
        assert_eq!(
            ret,
            Array::new(vec![
                BulkString::from("foo").into(),
                BulkString::from("bar").into(),
                BulkString::from("hello").into(),
                BulkString::from("world").into(),
            ])
            .into()
        );

        Ok(())
    }
}
