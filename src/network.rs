use crate::cmd::{Cmd, CmdExecutor};
use crate::{Backend, RespDecode, RespEncode, RespErr, RespFrame};
use anyhow::Result;
use bytes::BytesMut;
use futures::SinkExt;
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::info;

#[derive(Debug)]
struct RespFrameCodec;

impl Encoder<RespFrame> for RespFrameCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, item: RespFrame, dst: &mut BytesMut) -> Result<()> {
        let encoded = item.encode();
        dst.extend_from_slice(&encoded);

        Ok(())
    }
}

impl Decoder for RespFrameCodec {
    type Item = RespFrame;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<RespFrame>> {
        match RespFrame::decode(src) {
            Ok(frame) => Ok(Some(frame)),
            Err(RespErr::NotComplete) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

#[derive(Debug)]
struct RedisReq {
    frame: RespFrame,
    backend: Backend,
}

#[derive(Debug)]
struct RedisRsp {
    frame: RespFrame,
}

pub async fn handle_stream(stream: TcpStream, backend: Backend) -> Result<()> {
    let mut framed = Framed::new(stream, RespFrameCodec);

    loop {
        match framed.next().await {
            Some(Ok(frame)) => {
                info!("Received frame: {:?}", frame);

                let req = RedisReq {
                    frame,
                    backend: backend.clone(),
                };

                let rsp = handle_req(req).await?;

                info!("Sending response: {:?}", rsp.frame);
                framed.send(rsp.frame).await?;
            }
            Some(Err(e)) => return Err(e),
            None => return Ok(()),
        }
    }
}

async fn handle_req(req: RedisReq) -> Result<RedisRsp> {
    let (frame, backend) = (req.frame, req.backend);
    let cmd = Cmd::try_from(frame)?;

    info!("Execute command: {:?}", cmd);
    let frame = cmd.exec(&backend);

    Ok(RedisRsp { frame })
}
