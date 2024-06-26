use crate::resp::extract_fixed_data;
use crate::{RespDecode, RespEncode, RespErr};
use bytes::BytesMut;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct Null;

// The null data type represents non-existent values.
// _\r\n
impl RespEncode for Null {
    fn encode(self) -> Vec<u8> {
        b"_\r\n".to_vec()
    }
}

impl RespDecode for Null {
    const PREFIX: &'static str = "_";

    fn decode(buf: &mut BytesMut) -> Result<Self, RespErr> {
        extract_fixed_data(buf, "_\r\n", "Null")?;
        Ok(Null)
    }

    fn expect_len(_buf: &[u8]) -> Result<usize, RespErr> {
        Ok(3)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RespFrame;

    use anyhow::Result;

    #[test]
    fn test_null_encode() {
        let frame: RespFrame = Null.into();
        assert_eq!(frame.encode(), b"_\r\n");
    }

    #[test]
    fn test_null_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"_\r\n");

        let frame = Null::decode(&mut buf)?;
        assert_eq!(frame, Null);

        Ok(())
    }
}
