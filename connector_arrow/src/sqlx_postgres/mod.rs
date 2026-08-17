use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use sqlx::PgConnection;
use thiserror::Error;
use crate::api_async::AsyncConnector;
use crate::errors::ConnectorError;
use async_trait::async_trait;

pub mod types;
pub mod query;
pub mod append;
#[path = "../postgres/decimal.rs"]
pub mod decimal;

#[allow(dead_code)]
pub struct SqlxPostgresConnection {
    pub(crate) client: PgConnection,
}

impl SqlxPostgresConnection {
    pub fn new(client: PgConnection) -> Self {
        SqlxPostgresConnection { client }
    }

    pub fn unwrap(self) -> PgConnection {
        self.client
    }

    pub fn inner_mut(&mut self) -> &mut PgConnection {
        &mut self.client
    }
}

#[derive(Error, Debug)]
pub enum SqlxPostgresError {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

#[async_trait]
impl AsyncConnector for SqlxPostgresConnection {
    type Stmt<'conn> = query::SqlxStatement<'conn> where Self: 'conn;
    type Append<'conn> = append::SqlxAppender<'conn> where Self: 'conn;

    async fn query<'a>(&'a mut self, query: &str) -> Result<Self::Stmt<'a>, ConnectorError> {
        Ok(query::SqlxStatement {
            _client: &mut self.client,
            query: query.to_string(),
        })
    }

    async fn append<'a>(&'a mut self, table_name: &str) -> Result<Self::Append<'a>, ConnectorError> {
        let query = format!("COPY {} FROM STDIN WITH (FORMAT binary)", crate::util::escape::escaped_ident(table_name));
        let copy_in = self.client.copy_in_raw(&query).await.map_err(SqlxPostgresError::Sqlx)?;
        Ok(append::SqlxAppender::new(copy_in))
    }

    fn type_db_into_arrow(ty: &str) -> Option<DataType> {
        Some(match ty.to_lowercase().as_str() {
            "boolean" | "bool" => DataType::Boolean,
            "smallint" | "int2" => DataType::Int16,
            "integer" | "int4" => DataType::Int32,
            "bigint" | "int8" => DataType::Int64,
            "real" | "float4" => DataType::Float32,
            "double precision" | "float8" => DataType::Float64,
            "numeric" | "decimal" => DataType::Utf8,
            "timestamp" | "timestamp without time zone" => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            "timestamptz" | "timestamp with time zone" => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
            }
            "date" => DataType::Date32,
            "time" | "time without time zone" => DataType::Time64(TimeUnit::Microsecond),
            "interval" => DataType::Interval(IntervalUnit::MonthDayNano),
            "bytea" => DataType::Binary,
            "bit" | "bit varying" | "varbit" => DataType::Binary,
            "text" | "varchar" | "char" | "bpchar" => DataType::Utf8,
            _ if ty.starts_with("bit") => DataType::Binary,
            _ if ty.starts_with("varchar") | ty.starts_with("char") | ty.starts_with("bpchar") => {
                DataType::Utf8
            }
            _ if ty.starts_with("decimal") | ty.starts_with("numeric") => DataType::Utf8,
            _ => return None,
        })
    }

    fn type_arrow_into_db(ty: &DataType) -> Option<String> {
        Some(
            match ty {
                DataType::Null => "smallint",
                DataType::Boolean => "bool",
                DataType::Int8 => "smallint",
                DataType::Int16 => "smallint",
                DataType::Int32 => "integer",
                DataType::Int64 => "bigint",
                DataType::UInt8 => "smallint",
                DataType::UInt16 => "integer",
                DataType::UInt32 => "bigint",
                DataType::UInt64 => "decimal(20, 0)",
                DataType::Float16 => "real",
                DataType::Float32 => "real",
                DataType::Float64 => "double precision",
                DataType::Timestamp(_, _) => "bigint",
                DataType::Date32 => "integer",
                DataType::Date64 => "bigint",
                DataType::Time32(_) => "integer",
                DataType::Time64(_) => "bigint",
                DataType::Duration(_) => "bigint",
                DataType::Interval(_) => return None,
                DataType::Utf8 | DataType::LargeUtf8 => "text",
                DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => "bytea",
                DataType::Decimal32(..) | DataType::Decimal64(..) => return None,
                DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
                    return Some(format!("decimal({precision}, {scale}"));
                }
                _ => return None,
            }
            .into(),
        )
    }
}

