#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Protocol error: {message}.")]
    ProtocolError { message: String },

    #[error("Error occured at database: {message}.")]
    DatabaseError { message: String },

    #[error("Query error: {message}.")]
    QueryError { message: String },

    #[error("Unsupported type: {t}.")]
    UnsupportedTypeError { t: duckdb::types::Type },

    #[error("Unsupported parquet uri: {0}.")]
    UnsupportedParquetUri(String),

    #[error("Invalid request: {0}.")]
    InvalidRequest(String),

    #[error("Internal error: {message}.")]
    InternalError { message: String },
}

impl Error {
    pub const fn unsupported_type(t: duckdb::types::Type) -> Self {
        Error::UnsupportedTypeError { t: t }
    }

    pub fn internal<S: AsRef<str>>(message: S) -> Self {
        Error::InternalError {
            message: String::from(message.as_ref()),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<duckdb::Error> for Error {
    fn from(value: duckdb::Error) -> Self {
        Self::DatabaseError {
            message: value.to_string(),
        }
    }
}

impl From<duckdb::types::FromSqlError> for Error {
    fn from(value: duckdb::types::FromSqlError) -> Self {
        match value {
            duckdb::types::FromSqlError::InvalidType => Error::DatabaseError {
                message: String::from("invalid type"),
            },
            duckdb::types::FromSqlError::OutOfRange(range) => Error::DatabaseError {
                message: format!("value out of range ({})", range),
            },
            duckdb::types::FromSqlError::Other(err) => Error::DatabaseError {
                message: err.to_string(),
            },
            _ => Error::DatabaseError {
                message: String::from("unknown"),
            },
        }
    }
}
