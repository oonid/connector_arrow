#![allow(dead_code)]

use arrow::array::{RecordBatch};
use arrow::util::pretty::pretty_format_batches;
use connector_arrow::api_async::{
    AsyncAppend, AsyncConnector, AsyncResultReader, AsyncStatement,
};
use connector_arrow::api::{Connector, SchemaEdit};
use rand::SeedableRng;

use crate::util::{coerce_type};
use crate::{generator::generate_batch, spec::ArrowGenSpec};

pub async fn load_into_table<C, S>(
    conn: &mut C,
    setup_conn: &mut S,
    schema: arrow::datatypes::SchemaRef,
    batches: &[RecordBatch],
    table_name: &str,
) -> Result<(), connector_arrow::ConnectorError>
where
    C: AsyncConnector,
    S: Connector + SchemaEdit,
{
    // table drop
    match tokio::task::block_in_place(|| setup_conn.table_drop(table_name)) {
        Ok(_) | Err(connector_arrow::TableDropError::TableNonexistent) => (),
        Err(connector_arrow::TableDropError::Connector(e)) => return Err(e),
    }

    // table create
    match tokio::task::block_in_place(|| setup_conn.table_create(table_name, schema.clone())) {
        Ok(_) => (),
        Err(connector_arrow::TableCreateError::TableExists) => {
            panic!("table was just deleted, how can it exist now?")
        }
        Err(connector_arrow::TableCreateError::Connector(e)) => return Err(e),
    }

    // write into table
    {
        let mut appender = conn.append(table_name).await.unwrap();
        for batch in batches {
            appender.append(batch.clone()).await.unwrap();
        }
        appender.finish().await.unwrap();
    }

    Ok(())
}

pub async fn query_table<C: AsyncConnector>(
    conn: &mut C,
    table_name: &str,
    ident_quote_char: char,
) -> Result<(arrow::datatypes::SchemaRef, Vec<RecordBatch>), connector_arrow::ConnectorError> {
    let mut stmt = conn
        .query(&format!(
            "SELECT * FROM {ident_quote_char}{table_name}{ident_quote_char}"
        ))
        .await
        .unwrap();
    let mut reader = stmt.start(std::iter::empty::<&dyn connector_arrow::api::ArrowValue>()).await?;

    let schema = reader.get_schema();

    let mut batches = Vec::new();
    while let Some(batch) = reader.next_batch().await? {
        batches.push(batch);
    }
    
    Ok((schema?.clone(), batches))
}

pub async fn query_01<C: AsyncConnector>(conn: &mut C) {
    let query = "SELECT 1 as a, NULL as b";
    let mut stmt = conn.query(query).await.unwrap();
    let mut reader = stmt.start(std::iter::empty::<&dyn connector_arrow::api::ArrowValue>()).await.unwrap();
    
    let mut results = Vec::new();
    while let Some(batch) = reader.next_batch().await.unwrap() {
        results.push(batch);
    }

    similar_asserts::assert_eq!(
        pretty_format_batches(&results).unwrap().to_string(),
        "+---+---+\n\
         | a | b |\n\
         +---+---+\n\
         | 1 |   |\n\
         +---+---+"
    );
}

pub async fn roundtrip<C, S>(
    conn: &mut C,
    setup_conn: &mut S,
    table_name: &str,
    spec: ArrowGenSpec,
    ident_quote_char: char,
    nullable_results: bool,
) where
    C: AsyncConnector,
    S: Connector + SchemaEdit,
{
    let mut rng = rand_chacha::ChaCha8Rng::from_seed([0; 32]);
    let (schema, batches) = generate_batch(spec, &mut rng);

    load_into_table(conn, setup_conn, schema.clone(), &batches, table_name).await.unwrap();

    let override_nullable = if !nullable_results { Some(true) } else { None };
    let (schema_coerced, batches_coerced) =
        connector_arrow::util::coerce::coerce_batches(schema, &batches, coerce_type::<S>, override_nullable).unwrap();

    let (schema_query, batches_query) = query_table(conn, table_name, ident_quote_char).await.unwrap();

    similar_asserts::assert_eq!(schema_coerced, schema_query);
    similar_asserts::assert_eq!(batches_coerced, batches_query);
}
