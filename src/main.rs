mod service;

use std::net::SocketAddr;

use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let addr: SocketAddr = "0.0.0.0:50051".parse()?;

    let service = service::DuckDbService::new_server();

    log::info!("Start listening on {}", addr.to_string());
    Server::builder().add_service(service).serve(addr).await?;
    Ok(())
}
