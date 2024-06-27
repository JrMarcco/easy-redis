use anyhow::Result;
use easy_redis::{network, Backend};
use tokio::net::TcpListener;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "0.0.0.0:6379";
    info!("Listening on {}", addr);
    let listener = TcpListener::bind(addr).await?;
    let backend = Backend::new();

    loop {
        let (stream, remote) = listener.accept().await?;
        info!("Accepted connection from: {}", remote);

        let cloned_backed = backend.clone();
        tokio::spawn(async move {
            match network::handle_stream(stream, cloned_backed).await {
                Ok(_) => {
                    info!("Connection from {} exited", remote);
                }
                Err(e) => {
                    error!("Handle error for {}: {:?}", remote, e);
                }
            }
        });
    }
}
