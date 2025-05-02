use crate::error::{Error, Result};
use crate::proto;
use crate::uri::Uri;

pub struct Gduck {
    conn: duckdb::Connection,
}

impl Gduck {
    pub fn new(conn: duckdb::Connection) -> Self {
        Self { conn }
    }

    pub fn connect(conn: proto::Connect) -> Result<Gduck> {
        let conn = duckdb::Connection::open_with_flags(
            std::path::PathBuf::from(&conn.file_name),
            duckdb::Config::try_from(conn)?,
        )
        .map_err(crate::error::Error::from)?;
        Ok(Gduck::new(conn))
    }

    pub(crate) fn schema(
        schema: std::sync::Arc<duckdb::arrow::datatypes::Schema>,
    ) -> Result<proto::Schema> {
        let columns = schema
            .fields()
            .into_iter()
            .map(|field| {
                proto::DataType::try_from(field.data_type().to_owned()).map(|column_type| {
                    proto::Column {
                        name: field.name().to_owned(),
                        data_type: column_type as i32,
                    }
                })
            })
            .collect::<Result<prost::alloc::vec::Vec<proto::Column>>>()?;

        Ok(proto::Schema { columns })
    }

    pub fn execute<Q: AsRef<str>>(
        &self,
        sql: Q,
        params: proto::Params,
    ) -> Result<proto::response::QueryResult> {
        let params: duckdb::ParamsFromIter<Vec<proto::scalar_value::Kind>> = params.try_into()?;
        self.conn
            .execute(sql.as_ref(), params)
            .map_err(Error::from)?;
        Ok(proto::response::QueryResult {
            kind: Some(proto::response::query_result::Kind::Ok(())),
        })
    }

    pub fn query_value<Q: AsRef<str>>(
        &self,
        sql: Q,
        params: proto::Params,
    ) -> Result<proto::response::QueryResult> {
        let params: duckdb::ParamsFromIter<Vec<proto::scalar_value::Kind>> = params.try_into()?;
        self.conn.query_row_and_then(sql.as_ref(), params, |row| {
            let kind = row.get::<usize, proto::scalar_value::Kind>(0)?;
            Ok(proto::response::QueryResult {
                kind: Some(proto::response::query_result::Kind::Value(
                    proto::ScalarValue { kind: Some(kind) },
                )),
            })
        })
    }

    pub fn query_rows<Q: AsRef<str>>(
        &self,
        sql: Q,
        params: proto::Params,
    ) -> Result<proto::response::QueryResult> {
        let params: duckdb::ParamsFromIter<Vec<proto::scalar_value::Kind>> = params.try_into()?;

        let mut statement = self.conn.prepare(sql.as_ref())?;

        let rows = statement.query_and_then(params, |row| {
            let mut values = prost::alloc::vec![];
            for i in 0.. {
                match row.get::<usize, proto::scalar_value::Kind>(i) {
                    Ok(kind) => {
                        values.push(proto::ScalarValue { kind: Some(kind) });
                    }
                    Err(duckdb::Error::InvalidColumnIndex(_)) => {
                        break;
                    }
                    Err(err) => {
                        return Err(err);
                    }
                }
            }
            Ok(proto::Row { values })
        }).and_then(|rows| {
            rows.collect::<std::result::Result<prost::alloc::vec::Vec<proto::Row>, duckdb::Error>>()
        }).map_err(crate::error::Error::from)?;

        let schema = Self::schema(statement.schema())?;

        Ok(proto::response::QueryResult {
            kind: Some(proto::response::query_result::Kind::Rows(proto::Rows {
                schema: Some(schema),
                rows,
            })),
        })
    }

    pub fn create_table_as<T: AsRef<str>, Q: AsRef<str>>(
        &self,
        table: T,
        sql: Q,
        params: proto::Params,
    ) -> Result<proto::response::QueryResult> {
        let ctas_query = format!("CREATE TABLE {} AS {}", table.as_ref(), sql.as_ref());
        self.execute(ctas_query, params)
    }

    pub fn query_as_parquet<Q: AsRef<str>>(
        &self,
        sql: Q,
        params: proto::Params,
        uri: Uri,
    ) -> Result<proto::response::QueryResult> {
        let sql = sql.as_ref().trim();
        let query = format!(
            "COPY ({}) TO '{}' (FORMAT PARQUET)",
            sql.strip_suffix(";").unwrap_or(sql),
            uri
        );
        self.execute(query, params).and_then(|_| {
            Ok(proto::response::QueryResult {
                kind: Some(proto::response::query_result::Kind::ParquetFile(
                    proto::Location::try_from(uri)?,
                )),
            })
        })
    }
}
