use std::boxed::Box;
use std::pin::Pin;

use tokio_stream::{Stream, StreamExt};

use crate::proto;
use crate::proto::db_service_server as grpc;

#[derive(Debug)]
pub struct DuckDbService {}

#[tonic::async_trait]
impl grpc::DbService for DuckDbService {
    type TransactionStream =
        Pin<Box<dyn Stream<Item = Result<proto::Response, tonic::Status>> + Send + 'static>>;

    async fn transaction(
        &self,
        request: tonic::Request<tonic::Streaming<proto::Request>>,
    ) -> Result<tonic::Response<Self::TransactionStream>, tonic::Status> {
        let mut stream = request.into_inner();

        let gduck = stream.try_next().await?.and_then(|request| {
            request.message.map(|message| {
                if let proto::request::Message::Connect(c) = message {
                    crate::gduck::Gduck::connect(c)
                } else {
                    Err(crate::error::Error::ProtocolError {
                        message: String::from("Transaction must begin with Connect message."),
                    })
                }
            })
        });

        match gduck {
            Some(Ok(gduck)) => {
                let output = async_stream::stream! {
                    while let Some(request) = stream.try_next().await? {
                        if let Some(proto::request::Message::Query(q)) = request.message {
                            let query_result = match q.kind {
                                Some(proto::query::Kind::Execute(q)) => gduck.execute(q.query, q.params.unwrap_or_default()),
                                Some(proto::query::Kind::Value(q)) => gduck.query_value(q.query, q.params.unwrap_or_default()),
                                Some(proto::query::Kind::Rows(q)) => gduck.query_rows(q.query, q.params.unwrap_or_default()),
                                Some(proto::query::Kind::Ctas(ctas)) => gduck.create_table_as(ctas.table_name, ctas.query, ctas.params.unwrap_or_default()),
                                Some(proto::query::Kind::Parquet(pq)) => {
                                    match pq.location {
                                        Some(l) => crate::uri::Uri::try_from(l).and_then(|loc| gduck.query_as_parquet(pq.query, pq.params.unwrap_or_default(), loc)),
                                        None => Err(crate::error::Error::InvalidRequest(String::from("Parquet file location is required.")))
                                    }
                                }
                                kind => Err(crate::error::Error::ProtocolError { message: format!("Unknown query: {:?}", kind) })
                            };

                            match query_result {
                                Ok(result) => {
                                    yield Ok(proto::Response{ result: Some(proto::response::Result::Success(result))})
                                },
                                Err(err) => {
                                    yield Err(tonic::Status::new(tonic::Code::Internal, err.to_string()));
                                }
                            }
                        } else {
                            yield Err(tonic::Status::new(tonic::Code::Internal, "Unknown type of request received."));
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

    pub fn new_server() -> grpc::DbServiceServer<DuckDbService> {
        grpc::DbServiceServer::new(DuckDbService::new())
    }
}
