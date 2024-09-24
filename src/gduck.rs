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

    pub(crate) fn schema(statement: &duckdb::Statement<'_>) -> Result<proto::Schema> {
        let columns = statement
            .column_names()
            .into_iter()
            .enumerate()
            .map(|(index, name)| {
                proto::DataType::try_from(statement.column_type(index)).map(|column_type| {
                    proto::Column {
                        name: name,
                        data_type: column_type as i32,
                    }
                })
            })
            .collect::<Result<prost::alloc::vec::Vec<proto::Column>>>()?;

        Ok(proto::Schema { columns: columns })
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

        let schema = Self::schema(&statement)?;
        let n_columns = statement.column_count();

        let result = statement.query_and_then(params, |row| {
            let values =
                    (0..n_columns)
                        .map(|index| {
                            row.get::<usize, proto::scalar_value::Kind>(index)
                                .map(|kind| proto::ScalarValue { kind: Some(kind) })
                        })
                        .collect::<std::result::Result<
                            prost::alloc::vec::Vec<proto::ScalarValue>,
                            duckdb::Error,
                        >>();
            values.map(|values| proto::Row { values })
        });

        let rows = result.and_then(|res| {
            res.collect::<std::result::Result<prost::alloc::vec::Vec<proto::Row>, duckdb::Error>>()
                .map(|rows| proto::Rows {
                    schema: Some(schema),
                    rows: rows,
                })
        });

        rows.map(|rows| proto::response::QueryResult {
            kind: Some(proto::response::query_result::Kind::Rows(rows)),
        })
        .map_err(|err| crate::error::Error::from(err))
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
        let query = format!("COPY ({}) TO '{}' (FORMAT PARQUET)", sql.as_ref(), uri);
        self.execute(query, params).and_then(|_| {
            Ok(proto::response::QueryResult {
                kind: Some(proto::response::query_result::Kind::ParquetFile(
                    proto::Location::try_from(uri)?,
                )),
            })
        })
    }
}
