use crate::resp::extract_fixed_data;
use crate::{RespDecode, RespEncode, RespErr};
use bytes::BytesMut;

// #<t|f>\r\n
impl RespEncode for bool {
    fn encode(self) -> Vec<u8> {
        format!("#{}\r\n", if self { "t" } else { "f" }).into_bytes()
    }
}

impl RespDecode for bool {
    const PREFIX: &'static str = "#";

    fn decode(buf: &mut BytesMut) -> Result<Self, RespErr> {
        match extract_fixed_data(buf, "#t\r\n", "Boolean") {
            Ok(_) => Ok(true),
            Err(RespErr::NotComplete) => Err(RespErr::NotComplete),
            Err(_) => match extract_fixed_data(buf, "#f\r\n", "Boolean") {
                Ok(_) => Ok(false),
                Err(e) => Err(e),
            },
        }
    }

    fn expect_len(_buf: &[u8]) -> Result<usize, RespErr> {
        Ok(4)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RespFrame;

    use anyhow::Result;
    use bytes::BufMut;

    #[test]
    fn test_boolean_encode() {
        let frame: RespFrame = true.into();
        assert_eq!(frame.encode(), b"#t\r\n");

        let frame: RespFrame = false.into();
        assert_eq!(frame.encode(), b"#f\r\n");
    }

    #[test]
    fn test_boolean_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"#t\r\n");

        let frame = bool::decode(&mut buf)?;
        assert!(frame);

        buf.extend_from_slice(b"#f\r\n");

        let frame = bool::decode(&mut buf)?;
        assert!(!frame);

        buf.extend_from_slice(b"#t\r");

        let ret = bool::decode(&mut buf);
        assert_eq!(ret.unwrap_err(), RespErr::NotComplete);

        buf.put_u8(b'\n');

        let frame = bool::decode(&mut buf)?;
        assert!(frame);

        Ok(())
    }
}
