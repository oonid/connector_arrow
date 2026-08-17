use async_trait::async_trait;
use arrow::record_batch::RecordBatch;
use arrow::array::*;
use arrow::datatypes::{DataType, TimeUnit};
use crate::api_async::AsyncAppend;
use crate::errors::ConnectorError;
use super::SqlxPostgresError;
use bytes::BytesMut;

pub struct SqlxAppender<'conn> {
    pub(crate) copy_in: sqlx::postgres::PgCopyIn<&'conn mut sqlx::PgConnection>,
    header_sent: bool,
}

impl<'conn> SqlxAppender<'conn> {
    pub fn new(copy_in: sqlx::postgres::PgCopyIn<&'conn mut sqlx::PgConnection>) -> Self {
        Self {
            copy_in,
            header_sent: false,
        }
    }
}

#[async_trait]
impl<'conn> AsyncAppend<'conn> for SqlxAppender<'conn> {
    async fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        let mut buf = BytesMut::with_capacity(batch.num_rows() * 64);

        if !self.header_sent {
            // PGCOPY\n\377\r\n\0
            buf.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
            // flags
            buf.extend_from_slice(&0i32.to_be_bytes());
            // header extension length
            buf.extend_from_slice(&0i32.to_be_bytes());
            self.header_sent = true;
        }

        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();
        let cols = batch.columns();

        enum Accessor<'a> {
            Null,
            Boolean(&'a BooleanArray),
            Int8(&'a Int8Array),
            Int16(&'a Int16Array),
            Int32(&'a Int32Array),
            Int64(&'a Int64Array),
            UInt8(&'a UInt8Array),
            UInt16(&'a UInt16Array),
            UInt32(&'a UInt32Array),
            UInt64(&'a UInt64Array),
            Float16(&'a Float16Array),
            Float32(&'a Float32Array),
            Float64(&'a Float64Array),
            TimestampSecond(&'a TimestampSecondArray),
            TimestampMillisecond(&'a TimestampMillisecondArray),
            TimestampMicrosecond(&'a TimestampMicrosecondArray),
            TimestampNanosecond(&'a TimestampNanosecondArray),
            Date32(&'a Date32Array),
            Date64(&'a Date64Array),
            Time32Second(&'a Time32SecondArray),
            Time32Millisecond(&'a Time32MillisecondArray),
            Time64Microsecond(&'a Time64MicrosecondArray),
            Time64Nanosecond(&'a Time64NanosecondArray),
            DurationSecond(&'a DurationSecondArray),
            DurationMillisecond(&'a DurationMillisecondArray),
            DurationMicrosecond(&'a DurationMicrosecondArray),
            DurationNanosecond(&'a DurationNanosecondArray),
            Utf8(&'a StringArray),
            LargeUtf8(&'a LargeStringArray),
            Binary(&'a BinaryArray),
            LargeBinary(&'a LargeBinaryArray),
            FixedSizeBinary(&'a FixedSizeBinaryArray),
            Decimal128(&'a Decimal128Array),
            Decimal256(&'a Decimal256Array),
        }

        let mut accessors = Vec::with_capacity(num_cols);
        for col in 0..num_cols {
            let array = &cols[col];
            match array.data_type() {
                DataType::Null => accessors.push(Accessor::Null),
                DataType::Boolean => accessors.push(Accessor::Boolean(array.as_any().downcast_ref::<BooleanArray>().unwrap())),
                DataType::Int8 => accessors.push(Accessor::Int8(array.as_any().downcast_ref::<Int8Array>().unwrap())),
                DataType::Int16 => accessors.push(Accessor::Int16(array.as_any().downcast_ref::<Int16Array>().unwrap())),
                DataType::Int32 => accessors.push(Accessor::Int32(array.as_any().downcast_ref::<Int32Array>().unwrap())),
                DataType::Int64 => accessors.push(Accessor::Int64(array.as_any().downcast_ref::<Int64Array>().unwrap())),
                DataType::UInt8 => accessors.push(Accessor::UInt8(array.as_any().downcast_ref::<UInt8Array>().unwrap())),
                DataType::UInt16 => accessors.push(Accessor::UInt16(array.as_any().downcast_ref::<UInt16Array>().unwrap())),
                DataType::UInt32 => accessors.push(Accessor::UInt32(array.as_any().downcast_ref::<UInt32Array>().unwrap())),
                DataType::UInt64 => accessors.push(Accessor::UInt64(array.as_any().downcast_ref::<UInt64Array>().unwrap())),
                DataType::Float16 => accessors.push(Accessor::Float16(array.as_any().downcast_ref::<Float16Array>().unwrap())),
                DataType::Float32 => accessors.push(Accessor::Float32(array.as_any().downcast_ref::<Float32Array>().unwrap())),
                DataType::Float64 => accessors.push(Accessor::Float64(array.as_any().downcast_ref::<Float64Array>().unwrap())),
                DataType::Timestamp(TimeUnit::Second, _) => accessors.push(Accessor::TimestampSecond(array.as_any().downcast_ref::<TimestampSecondArray>().unwrap())),
                DataType::Timestamp(TimeUnit::Millisecond, _) => accessors.push(Accessor::TimestampMillisecond(array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap())),
                DataType::Timestamp(TimeUnit::Microsecond, _) => accessors.push(Accessor::TimestampMicrosecond(array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap())),
                DataType::Timestamp(TimeUnit::Nanosecond, _) => accessors.push(Accessor::TimestampNanosecond(array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap())),
                DataType::Date32 => accessors.push(Accessor::Date32(array.as_any().downcast_ref::<Date32Array>().unwrap())),
                DataType::Date64 => accessors.push(Accessor::Date64(array.as_any().downcast_ref::<Date64Array>().unwrap())),
                DataType::Time32(TimeUnit::Second) => accessors.push(Accessor::Time32Second(array.as_any().downcast_ref::<Time32SecondArray>().unwrap())),
                DataType::Time32(TimeUnit::Millisecond) => accessors.push(Accessor::Time32Millisecond(array.as_any().downcast_ref::<Time32MillisecondArray>().unwrap())),
                DataType::Time64(TimeUnit::Microsecond) => accessors.push(Accessor::Time64Microsecond(array.as_any().downcast_ref::<Time64MicrosecondArray>().unwrap())),
                DataType::Time64(TimeUnit::Nanosecond) => accessors.push(Accessor::Time64Nanosecond(array.as_any().downcast_ref::<Time64NanosecondArray>().unwrap())),
                DataType::Duration(TimeUnit::Second) => accessors.push(Accessor::DurationSecond(array.as_any().downcast_ref::<DurationSecondArray>().unwrap())),
                DataType::Duration(TimeUnit::Millisecond) => accessors.push(Accessor::DurationMillisecond(array.as_any().downcast_ref::<DurationMillisecondArray>().unwrap())),
                DataType::Duration(TimeUnit::Microsecond) => accessors.push(Accessor::DurationMicrosecond(array.as_any().downcast_ref::<DurationMicrosecondArray>().unwrap())),
                DataType::Duration(TimeUnit::Nanosecond) => accessors.push(Accessor::DurationNanosecond(array.as_any().downcast_ref::<DurationNanosecondArray>().unwrap())),
                DataType::Utf8 => accessors.push(Accessor::Utf8(array.as_any().downcast_ref::<StringArray>().unwrap())),
                DataType::LargeUtf8 => accessors.push(Accessor::LargeUtf8(array.as_any().downcast_ref::<LargeStringArray>().unwrap())),
                DataType::Binary => accessors.push(Accessor::Binary(array.as_any().downcast_ref::<BinaryArray>().unwrap())),
                DataType::LargeBinary => accessors.push(Accessor::LargeBinary(array.as_any().downcast_ref::<LargeBinaryArray>().unwrap())),
                DataType::FixedSizeBinary(_) => accessors.push(Accessor::FixedSizeBinary(array.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap())),
                DataType::Decimal128(_, _) => accessors.push(Accessor::Decimal128(array.as_any().downcast_ref::<Decimal128Array>().unwrap())),
                DataType::Decimal256(_, _) => accessors.push(Accessor::Decimal256(array.as_any().downcast_ref::<Decimal256Array>().unwrap())),
                _ => return Err(ConnectorError::NotSupported { connector_name: "sqlx_postgres", feature: "type" }),
            }
        }


        for row in 0..num_rows {
            buf.extend_from_slice(&(num_cols as i16).to_be_bytes());
            for col in 0..num_cols {
                let array = &cols[col];
                if array.is_null(row) || matches!(array.data_type(), DataType::Null) {
                    buf.extend_from_slice(&(-1i32).to_be_bytes());
                } else {
                    match &accessors[col] {
                        Accessor::Null => unreachable!(),
                        Accessor::Boolean(a) => {
                            buf.extend_from_slice(&1i32.to_be_bytes());
                            buf.extend_from_slice(&[if a.value(row) { 1 } else { 0 }]);
                        }
                        Accessor::Int8(a) => {
                            buf.extend_from_slice(&2i32.to_be_bytes());
                            buf.extend_from_slice(&(a.value(row) as i16).to_be_bytes());
                        }
                        Accessor::Int16(a) => {
                            buf.extend_from_slice(&2i32.to_be_bytes());
                            buf.extend_from_slice(&a.value(row).to_be_bytes());
                        }
                        Accessor::Int32(a) => {
                            buf.extend_from_slice(&4i32.to_be_bytes());
                            buf.extend_from_slice(&a.value(row).to_be_bytes());
                        }
                        Accessor::Int64(a) => {
                            buf.extend_from_slice(&8i32.to_be_bytes());
                            buf.extend_from_slice(&a.value(row).to_be_bytes());
                        }
                        Accessor::UInt8(a) => {
                            buf.extend_from_slice(&2i32.to_be_bytes());
                            buf.extend_from_slice(&(a.value(row) as i16).to_be_bytes());
                        }
                        Accessor::UInt16(a) => {
                            buf.extend_from_slice(&4i32.to_be_bytes());
                            buf.extend_from_slice(&(a.value(row) as i32).to_be_bytes());
                        }
                        Accessor::UInt32(a) => {
                            buf.extend_from_slice(&8i32.to_be_bytes());
                            buf.extend_from_slice(&(a.value(row) as i64).to_be_bytes());
                        }
                        Accessor::UInt64(a) => {
                            let start = buf.len();
                            buf.extend_from_slice(&[0; 4]);
                            crate::sqlx_postgres::decimal::i128_to_sql(a.value(row) as i128, 0, &mut buf);
                            let len = (buf.len() - start - 4) as i32;
                            buf[start..start + 4].copy_from_slice(&len.to_be_bytes());
                        }
                        Accessor::Float16(a) => {
                            buf.extend_from_slice(&4i32.to_be_bytes());
                            buf.extend_from_slice(&f32::from(a.value(row)).to_be_bytes());
                        }
                        Accessor::Float32(a) => {
                            buf.extend_from_slice(&4i32.to_be_bytes());
                            buf.extend_from_slice(&a.value(row).to_be_bytes());
                        }
                        Accessor::Float64(a) => {
                            buf.extend_from_slice(&8i32.to_be_bytes());
                            buf.extend_from_slice(&a.value(row).to_be_bytes());
                        }
                        Accessor::TimestampSecond(a) => {
                            buf.extend_from_slice(&8i32.to_be_bytes());
                            buf.extend_from_slice(&a.value(row).to_be_bytes());
                        }
                        Accessor::TimestampMillisecond(a) => {
                            buf.extend_from_slice(&8i32.to_be_bytes());
                            buf.extend_from_slice(&a.value(row).to_be_bytes());
                        }
                        Accessor::TimestampMicrosecond(a) => {
                            buf.extend_from_slice(&8i32.to_be_bytes());
                            buf.extend_from_slice(&a.value(row).to_be_bytes());
                        }
                        Accessor::TimestampNanosecond(a) => {
                            buf.extend_from_slice(&8i32.to_be_bytes());
                            buf.extend_from_slice(&a.value(row).to_be_bytes());
                        }
                        Accessor::Date32(a) => {
                            buf.extend_from_slice(&4i32.to_be_bytes());
                            buf.extend_from_slice(&a.value(row).to_be_bytes());
                        }
                        Accessor::Date64(a) => {
                            buf.extend_from_slice(&8i32.to_be_bytes());
                            buf.extend_from_slice(&a.value(row).to_be_bytes());
                        }
                        Accessor::Time32Second(a) => {
                            buf.extend_from_slice(&4i32.to_be_bytes());
                            buf.extend_from_slice(&a.value(row).to_be_bytes());
                        }
                        Accessor::Time32Millisecond(a) => {
                            buf.extend_from_slice(&4i32.to_be_bytes());
                            buf.extend_from_slice(&a.value(row).to_be_bytes());
                        }
                        Accessor::Time64Microsecond(a) => {
                            buf.extend_from_slice(&8i32.to_be_bytes());
                            buf.extend_from_slice(&a.value(row).to_be_bytes());
                        }
                        Accessor::Time64Nanosecond(a) => {
                            buf.extend_from_slice(&8i32.to_be_bytes());
                            buf.extend_from_slice(&a.value(row).to_be_bytes());
                        }
                        Accessor::DurationSecond(a) => {
                            buf.extend_from_slice(&8i32.to_be_bytes());
                            buf.extend_from_slice(&a.value(row).to_be_bytes());
                        }
                        Accessor::DurationMillisecond(a) => {
                            buf.extend_from_slice(&8i32.to_be_bytes());
                            buf.extend_from_slice(&a.value(row).to_be_bytes());
                        }
                        Accessor::DurationMicrosecond(a) => {
                            buf.extend_from_slice(&8i32.to_be_bytes());
                            buf.extend_from_slice(&a.value(row).to_be_bytes());
                        }
                        Accessor::DurationNanosecond(a) => {
                            buf.extend_from_slice(&8i32.to_be_bytes());
                            buf.extend_from_slice(&a.value(row).to_be_bytes());
                        }
                        Accessor::Utf8(a) => {
                            let bytes = a.value(row).as_bytes();
                            buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                            buf.extend_from_slice(bytes);
                        }
                        Accessor::LargeUtf8(a) => {
                            let bytes = a.value(row).as_bytes();
                            buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                            buf.extend_from_slice(bytes);
                        }
                        Accessor::Binary(a) => {
                            let bytes = a.value(row);
                            buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                            buf.extend_from_slice(bytes);
                        }
                        Accessor::LargeBinary(a) => {
                            let bytes = a.value(row);
                            buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                            buf.extend_from_slice(bytes);
                        }
                        Accessor::FixedSizeBinary(a) => {
                            let bytes = a.value(row);
                            buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                            buf.extend_from_slice(bytes);
                        }
                        Accessor::Decimal128(a) => {
                            if let DataType::Decimal128(_, scale) = a.data_type() {
                                let start = buf.len();
                                buf.extend_from_slice(&[0; 4]);
                                crate::sqlx_postgres::decimal::i128_to_sql(a.value(row), *scale, &mut buf);
                                let len = (buf.len() - start - 4) as i32;
                                buf[start..start + 4].copy_from_slice(&len.to_be_bytes());
                            }
                        }
                        Accessor::Decimal256(a) => {
                            if let DataType::Decimal256(_, scale) = a.data_type() {
                                let start = buf.len();
                                buf.extend_from_slice(&[0; 4]);
                                crate::sqlx_postgres::decimal::i256_to_sql(a.value(row), *scale, &mut buf);
                                let len = (buf.len() - start - 4) as i32;
                                buf[start..start + 4].copy_from_slice(&len.to_be_bytes());
                            }
                        }
                    }
                }
            }
        }

        if !buf.is_empty() {
            self.copy_in.send(buf).await.map_err(SqlxPostgresError::Sqlx)?;
        }

        Ok(())
    }

    async fn finish(mut self) -> Result<(), ConnectorError> {
        let mut buf = Vec::new();
        if !self.header_sent {
            // In case of empty batch stream
            buf.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
            buf.extend_from_slice(&0i32.to_be_bytes());
            buf.extend_from_slice(&0i32.to_be_bytes());
        }
        buf.extend_from_slice(&(-1i16).to_be_bytes());
        self.copy_in.send(buf).await.map_err(SqlxPostgresError::Sqlx)?;
        self.copy_in.finish().await.map_err(SqlxPostgresError::Sqlx)?;
        Ok(())
    }
}
