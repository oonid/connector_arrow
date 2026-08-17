use std::collections::HashMap;

use arrow::datatypes::{DataType, Field};
use sqlx::{postgres::PgTypeInfo, TypeInfo};

// Extract mapped DataType before calling this.

pub fn pg_field_to_arrow(
    name: String,
    _db_ty: &PgTypeInfo,
    nullable: bool,
    data_type: Option<DataType>,
) -> Result<Field, crate::errors::ConnectorError> {
    let metadata = HashMap::new();

    let data_type = data_type.ok_or_else(|| {
        crate::errors::ConnectorError::NotSupported {
            connector_name: "sqlx_postgres",
            feature: "unsupported postgres data type",
        }
    })?;

    Ok(Field::new(name, data_type, nullable).with_metadata(metadata))
}

pub fn pg_columns_to_arrow(columns: &[sqlx::postgres::PgColumn]) -> Result<std::sync::Arc<arrow::datatypes::Schema>, crate::errors::ConnectorError> {
    use sqlx::Column;
    let fields: Result<Vec<_>, _> = columns
        .iter()
        .map(|col| {
            // we will need to map PgTypeInfo to DataType using the SqlxPostgresConnection mapping logic
            let data_type = <crate::sqlx_postgres::SqlxPostgresConnection as crate::api_async::AsyncConnector>::type_db_into_arrow(col.type_info().name());
            pg_field_to_arrow(col.name().to_string(), col.type_info(), true, data_type)
        })
        .collect();
    Ok(std::sync::Arc::new(arrow::datatypes::Schema::new(fields?)))
}
