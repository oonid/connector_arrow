use async_trait::async_trait;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use crate::api::ArrowValue;
use crate::errors::ConnectorError;

#[async_trait]
pub trait AsyncConnector {
    type Stmt<'conn>: AsyncStatement<'conn>
    where
        Self: 'conn;
    type Append<'conn>: AsyncAppend<'conn>
    where
        Self: 'conn;

    async fn query<'a>(&'a mut self, query: &str) -> Result<Self::Stmt<'a>, ConnectorError>;
    async fn append<'a>(&'a mut self, table_name: &str) -> Result<Self::Append<'a>, ConnectorError>;

    fn type_db_into_arrow(database_ty: &str) -> Option<DataType>;
    fn type_arrow_into_db(ty: &DataType) -> Option<String>;
}

#[async_trait]
pub trait AsyncStatement<'conn> {
    type Reader<'stmt>: AsyncResultReader<'stmt>
    where
        Self: 'stmt;

    async fn start<'p, I>(&mut self, args: I) -> Result<Self::Reader<'_>, ConnectorError>
    where
        I: IntoIterator<Item = &'p dyn ArrowValue> + Send;
}

#[async_trait]
pub trait AsyncResultReader<'stmt> {
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError>;
    fn get_schema(&self) -> Result<SchemaRef, ConnectorError>;
}

#[async_trait]
pub trait AsyncAppend<'conn> {
    async fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError>;
    async fn finish(self) -> Result<(), ConnectorError>;
}
