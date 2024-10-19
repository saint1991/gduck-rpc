use chrono::{Datelike, Timelike};

tonic::include_proto!("gduck");

impl From<connect::Mode> for duckdb::AccessMode {
    fn from(value: connect::Mode) -> Self {
        match value {
            connect::Mode::Auto => duckdb::AccessMode::Automatic,
            connect::Mode::ReadWrite => duckdb::AccessMode::ReadWrite,
            connect::Mode::ReadOnly => duckdb::AccessMode::ReadOnly,
        }
    }
}

impl TryFrom<Connect> for duckdb::Config {
    type Error = crate::error::Error;

    fn try_from(value: Connect) -> crate::error::Result<Self> {
        duckdb::Config::default()
            .access_mode(value.mode().into())
            .map_err(crate::error::Error::from)
    }
}

impl TryFrom<Location> for crate::uri::Uri {
    type Error = crate::error::Error;

    fn try_from(value: Location) -> Result<Self, Self::Error> {
        match value.kind {
            Some(location::Kind::Local(local)) => Ok(crate::uri::Uri::LocalFileSystem(
                std::path::PathBuf::from(local.path),
            )),
            None => Err(crate::error::Error::ProtocolError {
                message: String::from("Parquet file location is required."),
            }),
        }
    }
}

impl TryFrom<crate::uri::Uri> for Location {
    type Error = crate::error::Error;

    fn try_from(uri: crate::uri::Uri) -> Result<Self, Self::Error> {
        match uri {
            crate::uri::Uri::LocalFileSystem(path) => Ok(Location {
                kind: Some(location::Kind::Local(location::LocalFile {
                    path: String::from(path.to_string_lossy()),
                })),
            }),
            _ => Err(crate::error::Error::UnsupportedParquetUri(uri.to_string())),
        }
    }
}

impl TryFrom<duckdb::types::Type> for DataType {
    type Error = crate::error::Error;

    fn try_from(value: duckdb::types::Type) -> Result<Self, Self::Error> {
        match value {
            duckdb::types::Type::Null => Ok(DataType::DatatypeNull),
            duckdb::types::Type::Boolean => Ok(DataType::DatatypeBool),
            duckdb::types::Type::TinyInt
            | duckdb::types::Type::SmallInt
            | duckdb::types::Type::Int
            | duckdb::types::Type::BigInt => Ok(DataType::DatatypeInt),
            duckdb::types::Type::UTinyInt
            | duckdb::types::Type::USmallInt
            | duckdb::types::Type::UInt
            | duckdb::types::Type::UBigInt => Ok(DataType::DatatypeUint),
            duckdb::types::Type::Float | duckdb::types::Type::Double => {
                Ok(DataType::DatatypeDouble)
            }
            duckdb::types::Type::Decimal => Ok(DataType::DatatypeDecimal),
            duckdb::types::Type::Text => Ok(DataType::DatatypeString),
            duckdb::types::Type::Timestamp => Ok(DataType::DatatypeDatetime),
            duckdb::types::Type::Date32 => Ok(DataType::DatatypeDate),
            duckdb::types::Type::Time64 => Ok(DataType::DatatypeTime),
            duckdb::types::Type::Interval => Ok(DataType::DatatypeInterval),
            other_type => Err(crate::error::Error::unsupported_type(other_type)),
        }
    }
}

impl TryFrom<duckdb::arrow::datatypes::DataType> for DataType {
    type Error = crate::error::Error;

    fn try_from(value: duckdb::arrow::datatypes::DataType) -> Result<Self, Self::Error> {
        match value {
            duckdb::arrow::datatypes::DataType::Null => Ok(DataType::DatatypeNull),
            duckdb::arrow::datatypes::DataType::Boolean => Ok(DataType::DatatypeBool),
            duckdb::arrow::datatypes::DataType::Int8
            | duckdb::arrow::datatypes::DataType::Int16
            | duckdb::arrow::datatypes::DataType::Int32
            | duckdb::arrow::datatypes::DataType::Int64 => Ok(DataType::DatatypeInt),
            duckdb::arrow::datatypes::DataType::UInt8
            | duckdb::arrow::datatypes::DataType::UInt16
            | duckdb::arrow::datatypes::DataType::UInt32
            | duckdb::arrow::datatypes::DataType::UInt64 => Ok(DataType::DatatypeUint),
            duckdb::arrow::datatypes::DataType::Float16
            | duckdb::arrow::datatypes::DataType::Float32
            | duckdb::arrow::datatypes::DataType::Float64 => Ok(DataType::DatatypeDouble),
            duckdb::arrow::datatypes::DataType::Decimal128(_, _)
            | duckdb::arrow::datatypes::DataType::Decimal256(_, _) => Ok(DataType::DatatypeDecimal),
            duckdb::arrow::datatypes::DataType::Utf8
            | duckdb::arrow::datatypes::DataType::Utf8View
            | duckdb::arrow::datatypes::DataType::LargeUtf8 => Ok(DataType::DatatypeString),
            duckdb::arrow::datatypes::DataType::Timestamp(_, _) => Ok(DataType::DatatypeDatetime),
            duckdb::arrow::datatypes::DataType::Date32 => Ok(DataType::DatatypeDate),
            duckdb::arrow::datatypes::DataType::Time64(_) => Ok(DataType::DatatypeTime),
            duckdb::arrow::datatypes::DataType::Interval(_) => Ok(DataType::DatatypeInterval),
            t => Err(crate::error::Error::UnsupportedTypeError {
                t: duckdb::types::Type::from(&t),
            }),
        }
    }
}

impl duckdb::types::ToSql for scalar_value::Kind {
    fn to_sql(&self) -> duckdb::Result<duckdb::types::ToSqlOutput<'_>> {
        match self {
            scalar_value::Kind::NullValue(_) => {
                Ok(duckdb::types::ToSqlOutput::from(duckdb::types::Null))
            }
            scalar_value::Kind::BoolValue(bool) => Ok(duckdb::types::ToSqlOutput::from(*bool)),
            scalar_value::Kind::IntValue(i) => Ok(duckdb::types::ToSqlOutput::from(*i)),
            scalar_value::Kind::UintValue(u) => Ok(duckdb::types::ToSqlOutput::from(*u)),
            scalar_value::Kind::DoubleValue(d) => Ok(duckdb::types::ToSqlOutput::from(*d)),
            scalar_value::Kind::DecimalValue(d) => {
                Ok(duckdb::types::ToSqlOutput::from(d.value.to_owned()))
            }
            scalar_value::Kind::StrValue(s) => Ok(duckdb::types::ToSqlOutput::from(s.to_owned())),
            scalar_value::Kind::DatetimeValue(dt) => u32::try_from(dt.nanos)
                .map_err(|err| duckdb::Error::ToSqlConversionFailure(Box::new(err)))
                .and_then(|nanos| {
                    chrono::DateTime::from_timestamp(dt.seconds, nanos).ok_or_else(|| {
                        duckdb::Error::ToSqlConversionFailure(Box::new(
                            crate::error::Error::InvalidRequest(format!(
                                "invalid timestamp {}",
                                dt
                            )),
                        ))
                    })
                })
                .map(|dt| duckdb::types::ToSqlOutput::from(dt.format("%F %T%.f").to_string())), // refer to https://github.com/duckdb/duckdb-rs/blob/main/crates/duckdb/src/types/chrono.rs#L51
            scalar_value::Kind::DateValue(dt) => {
                chrono::NaiveDate::from_ymd_opt(dt.year, dt.month, dt.day)
                    .ok_or_else(|| {
                        duckdb::Error::ToSqlConversionFailure(Box::new(
                            crate::error::Error::InvalidRequest(format!(
                                "invalid timestamp {}-{}-{}",
                                dt.year, dt.month, dt.day
                            )),
                        ))
                    })
                    .map(|dt| duckdb::types::ToSqlOutput::from(dt.format("%F").to_string()))
                // refer to https://github.com/duckdb/duckdb-rs/blob/main/crates/duckdb/src/types/chrono.rs#L17
            }
            scalar_value::Kind::TimeValue(time) => chrono::NaiveTime::from_hms_nano_opt(
                time.hours,
                time.minutes,
                time.seconds,
                time.nanos,
            )
            .ok_or_else(|| {
                duckdb::Error::ToSqlConversionFailure(Box::new(
                    crate::error::Error::InvalidRequest(format!(
                        "invalid timestamp {}:{}:{}.{}",
                        time.hours, time.minutes, time.seconds, time.nanos
                    )),
                ))
            })
            .map(|time| duckdb::types::ToSqlOutput::from(time.format("%T%.f").to_string())), // refer to https://github.com/duckdb/duckdb-rs/blob/main/crates/duckdb/src/types/chrono.rs#L34
            scalar_value::Kind::IntervalValue(interval) => Ok(
                duckdb::types::ToSqlOutput::Borrowed(duckdb::types::ValueRef::Interval {
                    months: interval.months,
                    days: interval.days,
                    nanos: interval.nanos,
                }),
            ),
        }
    }
}

impl TryInto<duckdb::ParamsFromIter<Vec<scalar_value::Kind>>> for Params {
    type Error = crate::error::Error;

    fn try_into(self) -> Result<duckdb::ParamsFromIter<Vec<scalar_value::Kind>>, Self::Error> {
        Ok(duckdb::params_from_iter(
            self.params
                .into_iter()
                .map(|param: ScalarValue| match param.kind {
                    Some(k) => Ok(k),
                    None => Err(crate::error::Error::InvalidRequest(format!(
                        "Invalid param: {:?}",
                        param
                    ))),
                })
                .collect::<Result<Vec<scalar_value::Kind>, crate::error::Error>>()?,
        ))
    }
}

impl duckdb::types::FromSql for scalar_value::Kind {
    fn column_result(value: duckdb::types::ValueRef<'_>) -> duckdb::types::FromSqlResult<Self> {
        match value {
            duckdb::types::ValueRef::Null => Ok(scalar_value::Kind::NullValue(
                prost_types::NullValue::NullValue as i32,
            )),
            duckdb::types::ValueRef::Boolean(v) => Ok(scalar_value::Kind::BoolValue(v)),
            duckdb::types::ValueRef::TinyInt(v) => Ok(scalar_value::Kind::IntValue(i64::from(v))),
            duckdb::types::ValueRef::SmallInt(v) => Ok(scalar_value::Kind::IntValue(i64::from(v))),
            duckdb::types::ValueRef::Int(v) => Ok(scalar_value::Kind::IntValue(i64::from(v))),
            duckdb::types::ValueRef::BigInt(v) => Ok(scalar_value::Kind::IntValue(v)),
            duckdb::types::ValueRef::HugeInt(v) => Err(duckdb::types::FromSqlError::OutOfRange(v)),
            duckdb::types::ValueRef::UTinyInt(v) => Ok(scalar_value::Kind::UintValue(u64::from(v))),
            duckdb::types::ValueRef::USmallInt(v) => {
                Ok(scalar_value::Kind::UintValue(u64::from(v)))
            }
            duckdb::types::ValueRef::UInt(v) => Ok(scalar_value::Kind::UintValue(u64::from(v))),
            duckdb::types::ValueRef::UBigInt(v) => Ok(scalar_value::Kind::UintValue(v)),
            duckdb::types::ValueRef::Float(v) => Ok(scalar_value::Kind::DoubleValue(f64::from(v))),
            duckdb::types::ValueRef::Double(v) => Ok(scalar_value::Kind::DoubleValue(v)),
            duckdb::types::ValueRef::Decimal(v) => Ok(scalar_value::Kind::DecimalValue(Decimal {
                value: prost::alloc::string::String::from(v.to_string()),
            })),
            duckdb::types::ValueRef::Timestamp(..) => chrono::NaiveDateTime::column_result(value)
                .and_then(|dt| {
                    let utc = dt.and_utc();
                    let seconds = utc.timestamp();
                    let nanos = utc
                        .timestamp_nanos_opt()
                        .unwrap_or_else(|| utc.timestamp_micros() * 1000)
                        % 1000000000;

                    i32::try_from(nanos)
                        .map_err(|_| {
                            duckdb::types::FromSqlError::OutOfRange(i128::from(
                                utc.timestamp_micros() * 1000,
                            ))
                        })
                        .map(|nanos| prost_types::Timestamp {
                            seconds: seconds,
                            nanos: nanos,
                        })
                })
                .map(|timestamp| scalar_value::Kind::DatetimeValue(timestamp)),
            duckdb::types::ValueRef::Date32(_) => {
                chrono::NaiveDate::column_result(value).map(|dt| {
                    scalar_value::Kind::DateValue(Date {
                        year: dt.year(),
                        month: dt.month(),
                        day: dt.day(),
                    })
                })
            }
            duckdb::types::ValueRef::Time64(..) => {
                chrono::NaiveTime::column_result(value).map(|time| {
                    scalar_value::Kind::TimeValue(Time {
                        hours: time.hour(),
                        minutes: time.minute(),
                        seconds: time.second(),
                        nanos: time.nanosecond(),
                    })
                })
            }
            duckdb::types::ValueRef::Interval {
                months,
                days,
                nanos,
            } => Ok(scalar_value::Kind::IntervalValue(Interval {
                months,
                days,
                nanos,
            })),
            duckdb::types::ValueRef::Text(v) => String::from_utf8(v.to_owned())
                .map(|str| scalar_value::Kind::StrValue(str))
                .map_err(|err| {
                    duckdb::types::FromSqlError::Other(Box::new(crate::error::Error::internal(
                        err.to_string(),
                    )))
                }),
            other => Err(duckdb::types::FromSqlError::Other(Box::new(
                crate::error::Error::unsupported_type(other.data_type()),
            ))),
        }
    }
}
