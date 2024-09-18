use std::boxed::Box;
use std::pin::Pin;

use tokio_stream::{Stream, StreamExt};

use gduck::db_service_server::{DbService, DbServiceServer};

pub mod gduck {
    tonic::include_proto!("gduck");
}

impl From<gduck::request::connect::Mode> for duckdb::AccessMode {
    fn from(value: gduck::request::connect::Mode) -> Self {
        match value {
            gduck::request::connect::Mode::Auto => duckdb::AccessMode::Automatic,
            gduck::request::connect::Mode::ReadWrite => duckdb::AccessMode::ReadWrite,
            gduck::request::connect::Mode::ReadOnly => duckdb::AccessMode::ReadOnly,
        }
    }
}

#[derive(Debug)]
pub struct DuckDbService {}

#[tonic::async_trait]
impl DbService for DuckDbService {
    type TransactionStream =
        Pin<Box<dyn Stream<Item = Result<gduck::Response, tonic::Status>> + Send + 'static>>;

    async fn transaction(
        &self,
        request: tonic::Request<tonic::Streaming<gduck::Request>>,
    ) -> Result<tonic::Response<Self::TransactionStream>, tonic::Status> {
        let mut stream = request.into_inner();

        let connection = stream.try_next().await?.and_then(|request| {
            request.message.map(|message| {
                log::info!("{:?}", message);

                if let gduck::request::Message::Connect(c) = message {
                    duckdb::Connection::open_with_flags(
                        &c.file_name,
                        duckdb::Config::default().access_mode(c.mode().into())?,
                    )
                    .map_err(|err| anyhow::Error::from(err))
                } else {
                    Err(anyhow::anyhow!(
                        "Transaction must begin with Connect message."
                    ))
                }
            })
        });

        match connection {
            Some(Ok(conn)) => {
                let output = async_stream::stream! {
                    while let Some(request) = stream.try_next().await? {
                        match request.message {
                            Some(gduck::request::Message::Query(q)) => {
                                let query = q.query;
                                log::info!("{}", &query);

                                let result = conn.query_row(&query, [], |row| { row.get::<usize, String>(0) })
                                    .map_err(|err| { tonic::Status::new(tonic::Code::Unknown, err.to_string()) })?;
                                yield Ok(gduck::Response{result: Some(gduck::response::QueryResult{result: format!("{:?}", result)})});
                            },
                            _ => {
                                yield Err(tonic::Status::new(tonic::Code::Internal, "Unknown type of request received."));
                            }
                        }
                    }
                    log::info!("DONE");
                    yield Err(tonic::Status::ok("Completed successfully."))
                };
                Ok(tonic::Response::new(
                    Box::pin(output) as Self::TransactionStream
                ))
            }
            Some(Err(err)) => Err(tonic::Status::internal(err.to_string())),
            None => Err(tonic::Status::ok("No query was sent.")),
        }
    }
}

impl DuckDbService {
    pub fn new() -> DuckDbService {
        DuckDbService {}
    }

    pub fn new_server() -> DbServiceServer<DuckDbService> {
        DbServiceServer::new(DuckDbService::new())
    }
}
