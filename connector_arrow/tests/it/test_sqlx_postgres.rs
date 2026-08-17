use connector_arrow::sqlx_postgres::SqlxPostgresConnection;
use connector_arrow::postgres::PostgresConnection;

struct DropWrapper<T>(Option<T>);
impl<T> Drop for DropWrapper<T> {
    fn drop(&mut self) {
        if let Some(t) = self.0.take() {
            tokio::task::block_in_place(move || drop(t));
        }
    }
}

use crate::spec;

async fn get_db_url() -> String {
    if let Ok(url) = std::env::var("POSTGRES_URL") {
        return url;
    }
    
    static DB_URL: tokio::sync::OnceCell<String> = tokio::sync::OnceCell::const_new();
    DB_URL.get_or_init(|| async {
        let mut config = postg::config::Config::default();
        config.temporary = true;
        let db = postg::engine::Postg::start(config).await.unwrap();
        let url = db.connection_string();
        Box::leak(Box::new(db)); // Leak to keep DB alive for the duration of tests
        url
    }).await.clone()
}

async fn init() -> (SqlxPostgresConnection, PostgresConnection) {
    let _ = env_logger::builder().is_test(true).try_init();

    let dburl = get_db_url().await;
    use sqlx::Connection;
    let conn = sqlx::postgres::PgConnection::connect(&dburl).await.unwrap();
    
    let dburl_clone = dburl.clone();
    let client = tokio::task::block_in_place(move || postgres::Client::connect(&dburl_clone, postgres::NoTls).unwrap());
    
    (SqlxPostgresConnection::new(conn), PostgresConnection::new(client))
}

#[postg::test]
async fn query_01(url: String) {
    let _ = env_logger::builder().is_test(true).try_init();
    use sqlx::Connection;
    let conn = sqlx::postgres::PgConnection::connect(&url).await.unwrap();
    let mut conn = SqlxPostgresConnection::new(conn);
    super::tests_async::query_01(&mut conn).await;
}



#[rstest::rstest]
#[case::empty("roundtrip_async::empty", spec::empty())]
#[case::null_bool("roundtrip_async::null_bool", spec::null_bool())]
#[case::int("roundtrip_async::int", spec::int())]
#[case::uint("roundtrip_async::uint", spec::uint())]
#[case::float("roundtrip_async::float", spec::float())]
#[case::decimal("roundtrip_async::decimal", spec::decimal())]
#[case::timestamp("roundtrip_async::timestamp", spec::timestamp())]
#[case::date("roundtrip_async::date", spec::date())]
#[case::time("roundtrip_async::time", spec::time())]
#[case::duration("roundtrip_async::duration", spec::duration())]
#[case::utf8("roundtrip_async::utf8", spec::utf8_large())]
#[case::binary("roundtrip_async::binary", spec::binary_large())]
#[tokio::test(flavor = "multi_thread")]
async fn roundtrip(#[case] table_name: &str, #[case] spec: spec::ArrowGenSpec) {
    let (mut conn, setup_conn) = init().await;
    let mut setup_conn = DropWrapper(Some(setup_conn));
    super::tests_async::roundtrip(&mut conn, setup_conn.0.as_mut().unwrap(), table_name, spec, '"', false).await;
}

