use async_trait::async_trait;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use crate::api_async::{AsyncStatement, AsyncResultReader};
use crate::api::ArrowValue;
use crate::errors::ConnectorError;
use futures::stream::BoxStream;
use futures::StreamExt;
use sqlx::{Executor, Statement, Row, ValueRef, TypeInfo};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::array::*;
use std::convert::TryInto;

#[allow(dead_code)]
pub struct SqlxStatement<'conn> {
    pub(crate) _client: &'conn mut sqlx::PgConnection,
    pub(crate) query: String,
}

#[async_trait]
impl<'conn> AsyncStatement<'conn> for SqlxStatement<'conn> {
    type Reader<'stmt> = SqlxReader<'stmt> where Self: 'stmt;
    async fn start<'p, I>(&mut self, _args: I) -> Result<Self::Reader<'_>, ConnectorError>
    where
        I: IntoIterator<Item = &'p dyn ArrowValue> + Send,
    {
        let stmt = self._client.prepare(&self.query).await
            .map_err(crate::sqlx_postgres::SqlxPostgresError::Sqlx)?;
        
        let schema = crate::sqlx_postgres::types::pg_columns_to_arrow(stmt.columns())?;

        let mut query = sqlx::query(&self.query);
        for arg in _args {
            match arg.get_data_type() {
                DataType::Boolean => { query = query.bind(*arg.as_any().downcast_ref::<bool>().unwrap()); }
                DataType::Int16 => { query = query.bind(*arg.as_any().downcast_ref::<i16>().unwrap()); }
                DataType::Int32 => { query = query.bind(*arg.as_any().downcast_ref::<i32>().unwrap()); }
                DataType::Int64 => { query = query.bind(*arg.as_any().downcast_ref::<i64>().unwrap()); }
                DataType::Float32 => { query = query.bind(*arg.as_any().downcast_ref::<f32>().unwrap()); }
                DataType::Float64 => { query = query.bind(*arg.as_any().downcast_ref::<f64>().unwrap()); }
                DataType::Utf8 => { query = query.bind(arg.as_any().downcast_ref::<String>().unwrap().clone()); }
                DataType::Binary => { query = query.bind(arg.as_any().downcast_ref::<Vec<u8>>().unwrap().clone()); }
                _ => return Err(ConnectorError::NotSupported { connector_name: "sqlx_postgres", feature: "unsupported parameter type" }),
            }
        }

        let rows = query.fetch(&mut *self._client);

        Ok(SqlxReader {
            schema,
            rows,
        })
    }
}

pub struct SqlxReader<'stmt> {
    schema: SchemaRef,
    rows: BoxStream<'stmt, Result<sqlx::postgres::PgRow, sqlx::Error>>,
}

#[async_trait]
impl<'stmt> AsyncResultReader<'stmt> for SqlxReader<'stmt> {
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        let mut builders: Vec<Box<dyn arrow::array::ArrayBuilder>> = self.schema
            .fields()
            .iter()
            .map(|f| arrow::array::make_builder(f.data_type(), 1024))
            .collect();

        enum BuilderMut<'a> {
            Null,
            Boolean(&'a mut BooleanBuilder),
            Int8(&'a mut Int8Builder),
            Int16(&'a mut Int16Builder),
            Int32(&'a mut Int32Builder),
            Int64(&'a mut Int64Builder),
            UInt8(&'a mut UInt8Builder),
            UInt16(&'a mut UInt16Builder),
            UInt32(&'a mut UInt32Builder),
            UInt64(&'a mut UInt64Builder),
            Float16(&'a mut Float16Builder),
            Float32(&'a mut Float32Builder),
            Float64(&'a mut Float64Builder),
            TimestampSecond(&'a mut TimestampSecondBuilder),
            TimestampMillisecond(&'a mut TimestampMillisecondBuilder),
            TimestampMicrosecond(&'a mut TimestampMicrosecondBuilder),
            TimestampNanosecond(&'a mut TimestampNanosecondBuilder),
            Date32(&'a mut Date32Builder),
            Date64(&'a mut Date64Builder),
            Time32Second(&'a mut Time32SecondBuilder),
            Time32Millisecond(&'a mut Time32MillisecondBuilder),
            Time64Microsecond(&'a mut Time64MicrosecondBuilder),
            Time64Nanosecond(&'a mut Time64NanosecondBuilder),
            DurationSecond(&'a mut DurationSecondBuilder),
            DurationMillisecond(&'a mut DurationMillisecondBuilder),
            DurationMicrosecond(&'a mut DurationMicrosecondBuilder),
            DurationNanosecond(&'a mut DurationNanosecondBuilder),
            Utf8(&'a mut StringBuilder),
            LargeUtf8(&'a mut LargeStringBuilder),
            Binary(&'a mut BinaryBuilder),
            LargeBinary(&'a mut LargeBinaryBuilder),
            FixedSizeBinary(&'a mut FixedSizeBinaryBuilder),
            Decimal128(&'a mut Decimal128Builder),
            Decimal256(&'a mut Decimal256Builder),
        }

        let mut typed_builders: Vec<BuilderMut> = Vec::with_capacity(builders.len());
        for (field, b) in self.schema.fields().iter().zip(builders.iter_mut()) {
            match field.data_type() {
                DataType::Null => typed_builders.push(BuilderMut::Null),
                DataType::Boolean => typed_builders.push(BuilderMut::Boolean(b.as_any_mut().downcast_mut().unwrap())),
                DataType::Int8 => typed_builders.push(BuilderMut::Int8(b.as_any_mut().downcast_mut().unwrap())),
                DataType::Int16 => typed_builders.push(BuilderMut::Int16(b.as_any_mut().downcast_mut().unwrap())),
                DataType::Int32 => typed_builders.push(BuilderMut::Int32(b.as_any_mut().downcast_mut().unwrap())),
                DataType::Int64 => typed_builders.push(BuilderMut::Int64(b.as_any_mut().downcast_mut().unwrap())),
                DataType::UInt8 => typed_builders.push(BuilderMut::UInt8(b.as_any_mut().downcast_mut().unwrap())),
                DataType::UInt16 => typed_builders.push(BuilderMut::UInt16(b.as_any_mut().downcast_mut().unwrap())),
                DataType::UInt32 => typed_builders.push(BuilderMut::UInt32(b.as_any_mut().downcast_mut().unwrap())),
                DataType::UInt64 => typed_builders.push(BuilderMut::UInt64(b.as_any_mut().downcast_mut().unwrap())),
                DataType::Float16 => typed_builders.push(BuilderMut::Float16(b.as_any_mut().downcast_mut().unwrap())),
                DataType::Float32 => typed_builders.push(BuilderMut::Float32(b.as_any_mut().downcast_mut().unwrap())),
                DataType::Float64 => typed_builders.push(BuilderMut::Float64(b.as_any_mut().downcast_mut().unwrap())),
                DataType::Timestamp(TimeUnit::Second, _) => typed_builders.push(BuilderMut::TimestampSecond(b.as_any_mut().downcast_mut().unwrap())),
                DataType::Timestamp(TimeUnit::Millisecond, _) => typed_builders.push(BuilderMut::TimestampMillisecond(b.as_any_mut().downcast_mut().unwrap())),
                DataType::Timestamp(TimeUnit::Microsecond, _) => typed_builders.push(BuilderMut::TimestampMicrosecond(b.as_any_mut().downcast_mut().unwrap())),
                DataType::Timestamp(TimeUnit::Nanosecond, _) => typed_builders.push(BuilderMut::TimestampNanosecond(b.as_any_mut().downcast_mut().unwrap())),
                DataType::Date32 => typed_builders.push(BuilderMut::Date32(b.as_any_mut().downcast_mut().unwrap())),
                DataType::Date64 => typed_builders.push(BuilderMut::Date64(b.as_any_mut().downcast_mut().unwrap())),
                DataType::Time32(TimeUnit::Second) => typed_builders.push(BuilderMut::Time32Second(b.as_any_mut().downcast_mut().unwrap())),
                DataType::Time32(TimeUnit::Millisecond) => typed_builders.push(BuilderMut::Time32Millisecond(b.as_any_mut().downcast_mut().unwrap())),
                DataType::Time64(TimeUnit::Microsecond) => typed_builders.push(BuilderMut::Time64Microsecond(b.as_any_mut().downcast_mut().unwrap())),
                DataType::Time64(TimeUnit::Nanosecond) => typed_builders.push(BuilderMut::Time64Nanosecond(b.as_any_mut().downcast_mut().unwrap())),
                DataType::Duration(TimeUnit::Second) => typed_builders.push(BuilderMut::DurationSecond(b.as_any_mut().downcast_mut().unwrap())),
                DataType::Duration(TimeUnit::Millisecond) => typed_builders.push(BuilderMut::DurationMillisecond(b.as_any_mut().downcast_mut().unwrap())),
                DataType::Duration(TimeUnit::Microsecond) => typed_builders.push(BuilderMut::DurationMicrosecond(b.as_any_mut().downcast_mut().unwrap())),
                DataType::Duration(TimeUnit::Nanosecond) => typed_builders.push(BuilderMut::DurationNanosecond(b.as_any_mut().downcast_mut().unwrap())),
                DataType::Utf8 => typed_builders.push(BuilderMut::Utf8(b.as_any_mut().downcast_mut().unwrap())),
                DataType::LargeUtf8 => typed_builders.push(BuilderMut::LargeUtf8(b.as_any_mut().downcast_mut().unwrap())),
                DataType::Binary => typed_builders.push(BuilderMut::Binary(b.as_any_mut().downcast_mut().unwrap())),
                DataType::LargeBinary => typed_builders.push(BuilderMut::LargeBinary(b.as_any_mut().downcast_mut().unwrap())),
                DataType::FixedSizeBinary(_) => typed_builders.push(BuilderMut::FixedSizeBinary(b.as_any_mut().downcast_mut().unwrap())),
                DataType::Decimal128(_, _) => typed_builders.push(BuilderMut::Decimal128(b.as_any_mut().downcast_mut().unwrap())),
                DataType::Decimal256(_, _) => typed_builders.push(BuilderMut::Decimal256(b.as_any_mut().downcast_mut().unwrap())),
                _ => return Err(ConnectorError::NotSupported { connector_name: "sqlx_postgres", feature: "unsupported data type" }),
            }
        }

        const DUR_1970_TO_2000_DAYS: i32 = 10957;
        const DUR_1970_TO_2000_SEC: i64 = DUR_1970_TO_2000_DAYS as i64 * 24 * 60 * 60;

        let mut row_count = 0;
        while row_count < 1024 {
            match self.rows.next().await {
                Some(Ok(row)) => {
                    for (i, _field) in self.schema.fields().iter().enumerate() {
                        let raw = row.try_get_raw(i).map_err(crate::sqlx_postgres::SqlxPostgresError::Sqlx)?;
                        if raw.is_null() {
                            match &mut typed_builders[i] {
                                BuilderMut::Null => {},
                                BuilderMut::Boolean(b) => b.append_null(),
                                BuilderMut::Int8(b) => b.append_null(),
                                BuilderMut::Int16(b) => b.append_null(),
                                BuilderMut::Int32(b) => b.append_null(),
                                BuilderMut::Int64(b) => b.append_null(),
                                BuilderMut::UInt8(b) => b.append_null(),
                                BuilderMut::UInt16(b) => b.append_null(),
                                BuilderMut::UInt32(b) => b.append_null(),
                                BuilderMut::UInt64(b) => b.append_null(),
                                BuilderMut::Float16(b) => b.append_null(),
                                BuilderMut::Float32(b) => b.append_null(),
                                BuilderMut::Float64(b) => b.append_null(),
                                BuilderMut::TimestampSecond(b) => b.append_null(),
                                BuilderMut::TimestampMillisecond(b) => b.append_null(),
                                BuilderMut::TimestampMicrosecond(b) => b.append_null(),
                                BuilderMut::TimestampNanosecond(b) => b.append_null(),
                                BuilderMut::Date32(b) => b.append_null(),
                                BuilderMut::Date64(b) => b.append_null(),
                                BuilderMut::Time32Second(b) => b.append_null(),
                                BuilderMut::Time32Millisecond(b) => b.append_null(),
                                BuilderMut::Time64Microsecond(b) => b.append_null(),
                                BuilderMut::Time64Nanosecond(b) => b.append_null(),
                                BuilderMut::DurationSecond(b) => b.append_null(),
                                BuilderMut::DurationMillisecond(b) => b.append_null(),
                                BuilderMut::DurationMicrosecond(b) => b.append_null(),
                                BuilderMut::DurationNanosecond(b) => b.append_null(),
                                BuilderMut::Utf8(b) => b.append_null(),
                                BuilderMut::LargeUtf8(b) => b.append_null(),
                                BuilderMut::Binary(b) => b.append_null(),
                                BuilderMut::LargeBinary(b) => b.append_null(),
                                BuilderMut::FixedSizeBinary(b) => b.append_null(),
                                BuilderMut::Decimal128(b) => b.append_null(),
                                BuilderMut::Decimal256(b) => b.append_null(),
                            }
                            continue;
                        }

                        let bytes = raw.as_bytes().unwrap();
                        let is_text = raw.format() == sqlx::postgres::PgValueFormat::Text;

                        match &mut typed_builders[i] {
                            BuilderMut::Null => {},
                            BuilderMut::Boolean(b) => {
                                let v = if is_text { bytes == b"t" } else { bytes[0] != 0 };
                                b.append_value(v);
                            }
                            BuilderMut::Int8(b) => b.append_value(i16::from_be_bytes(bytes.try_into().unwrap()) as i8),
                            BuilderMut::Int16(b) => b.append_value(i16::from_be_bytes(bytes.try_into().unwrap())),
                            BuilderMut::Int32(b) => b.append_value(i32::from_be_bytes(bytes.try_into().unwrap())),
                            BuilderMut::Int64(b) => b.append_value(i64::from_be_bytes(bytes.try_into().unwrap())),
                            BuilderMut::UInt8(b) => b.append_value(i16::from_be_bytes(bytes.try_into().unwrap()) as u8),
                            BuilderMut::UInt16(b) => b.append_value(i32::from_be_bytes(bytes.try_into().unwrap()) as u16),
                            BuilderMut::UInt32(b) => b.append_value(i64::from_be_bytes(bytes.try_into().unwrap()) as u32),
                            BuilderMut::UInt64(b) => {
                                let v = if is_text {
                                    std::str::from_utf8(bytes).unwrap().parse::<u64>().unwrap()
                                } else {
                                    let s = crate::sqlx_postgres::decimal::from_sql(bytes).unwrap();
                                    s.parse::<u64>().unwrap()
                                };
                                b.append_value(v);
                            }
                            BuilderMut::Float16(_) => unimplemented!("Float16 not supported"),
                            BuilderMut::Float32(b) => b.append_value(f32::from_be_bytes(bytes.try_into().unwrap())),
                            BuilderMut::Float64(b) => b.append_value(f64::from_be_bytes(bytes.try_into().unwrap())),
                            BuilderMut::TimestampSecond(b) => b.append_value(i64::from_be_bytes(bytes.try_into().unwrap())),
                            BuilderMut::TimestampMillisecond(b) => b.append_value(i64::from_be_bytes(bytes.try_into().unwrap())),
                            BuilderMut::TimestampMicrosecond(b) => b.append_value(i64::from_be_bytes(bytes.try_into().unwrap()) + DUR_1970_TO_2000_SEC * 1_000_000),
                            BuilderMut::TimestampNanosecond(b) => b.append_value(i64::from_be_bytes(bytes.try_into().unwrap())),
                            BuilderMut::Date32(b) => b.append_value(i32::from_be_bytes(bytes.try_into().unwrap()) + DUR_1970_TO_2000_DAYS),
                            BuilderMut::Date64(b) => b.append_value(i64::from_be_bytes(bytes.try_into().unwrap())),
                            BuilderMut::Time32Second(b) => b.append_value(i32::from_be_bytes(bytes.try_into().unwrap())),
                            BuilderMut::Time32Millisecond(b) => b.append_value(i32::from_be_bytes(bytes.try_into().unwrap())),
                            BuilderMut::Time64Microsecond(b) => b.append_value(i64::from_be_bytes(bytes.try_into().unwrap())),
                            BuilderMut::Time64Nanosecond(b) => b.append_value(i64::from_be_bytes(bytes.try_into().unwrap())),
                            BuilderMut::DurationSecond(b) => b.append_value(i64::from_be_bytes(bytes.try_into().unwrap())),
                            BuilderMut::DurationMillisecond(b) => b.append_value(i64::from_be_bytes(bytes.try_into().unwrap())),
                            BuilderMut::DurationMicrosecond(b) => b.append_value(i64::from_be_bytes(bytes.try_into().unwrap())),
                            BuilderMut::DurationNanosecond(b) => b.append_value(i64::from_be_bytes(bytes.try_into().unwrap())),
                            BuilderMut::Utf8(b) => {
                                if !is_text && raw.type_info().name().eq_ignore_ascii_case("numeric") {
                                    b.append_value(crate::sqlx_postgres::decimal::from_sql(bytes).unwrap());
                                } else {
                                    b.append_value(std::str::from_utf8(bytes).unwrap());
                                }
                            },
                            BuilderMut::LargeUtf8(b) => {
                                if !is_text && raw.type_info().name().eq_ignore_ascii_case("numeric") {
                                    b.append_value(crate::sqlx_postgres::decimal::from_sql(bytes).unwrap());
                                } else {
                                    b.append_value(std::str::from_utf8(bytes).unwrap());
                                }
                            },
                            BuilderMut::Binary(b) => b.append_value(bytes),
                            BuilderMut::LargeBinary(b) => b.append_value(bytes),
                            BuilderMut::FixedSizeBinary(b) => b.append_value(bytes).unwrap(),
                            BuilderMut::Decimal128(b) => {
                                let s = if is_text {
                                    std::str::from_utf8(bytes).unwrap().to_string()
                                } else {
                                    crate::sqlx_postgres::decimal::from_sql(bytes).unwrap()
                                };
                                let v: i128 = s.replace(".", "").parse().unwrap();
                                b.append_value(v);
                            }
                            BuilderMut::Decimal256(_) => panic!("Decimal256 decoding not fully implemented"),
                        }
                    }
                    row_count += 1;
                }
                Some(Err(e)) => return Err(crate::sqlx_postgres::SqlxPostgresError::Sqlx(e).into()),
                None => break,
            }
        }

        if row_count == 0 {
            return Ok(None);
        }

        let arrays = builders.into_iter().map(|mut b| b.finish()).collect::<Vec<_>>();
        Ok(Some(RecordBatch::try_new(self.schema.clone(), arrays)?))
    }
    fn get_schema(&self) -> Result<SchemaRef, ConnectorError> {
        Ok(self.schema.clone())
    }
}
