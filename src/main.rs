mod error;
mod gduck;
mod proto;
mod service;
mod uri;

use std::net::SocketAddr;

use clap::Parser;
use tonic::transport::Server;

/// gRPC server for duckdb service
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Bind address
    #[arg(short, long, default_value = "0.0.0.0")]
    bind_address: String,

    /// Port number to listen to
    #[arg(short, long, default_value_t = 50051)]
    port: i32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    env_logger::init();
    let addr: SocketAddr = format!("{}:{}", args.bind_address, args.port).parse()?;

    let service = service::DuckDbService::new_server();

    log::info!("Start listening on {}", addr.to_string());
    Server::builder().add_service(service).serve(addr).await?;
    Ok(())
}
