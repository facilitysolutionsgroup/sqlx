use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
    row::Row,
};
use sqlx_odbc::OdbcConnectOptions;

#[tokio::test]
#[ignore = "manual cleanup of this development session's failed scratch tests"]
async fn cleanup_failed_scratch_tests() {
    use sqlx_core::sql_str::AssertSqlSafe;
    let mut connection =
        OdbcConnectOptions::new(std::env::var("ODBC_TEST_CONNECTION_STRING").unwrap())
            .statement_cache_capacity(0)
            .connect()
            .await
            .unwrap();
    let rows = (&mut connection).fetch_all("SELECT TBL FROM SYSPROGRESS.SYSTABLES WHERE OWNER = 'PUB' AND (TBL LIKE 'sqlx_test_1878070_%' OR TBL LIKE 'sqlx_test_1879286_%')").await.unwrap();
    for row in rows {
        let table: String = row.try_get(0).unwrap();
        assert!(table
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_'));
        (&mut connection)
            .execute(AssertSqlSafe(format!("DROP TABLE PUB.{table}")))
            .await
            .unwrap();
    }
    connection.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires OpenEdge ODBC_TEST_CONNECTION_STRING and scratch-table privileges"]
async fn openedge_dml_counts_and_rollback() {
    use sqlx_core::sql_str::AssertSqlSafe;
    let options = OdbcConnectOptions::new(std::env::var("ODBC_TEST_CONNECTION_STRING").unwrap());
    let mut connection = options.connect().await.unwrap();
    let mut cleanup = options.connect().await.unwrap();
    let table = format!(
        "PUB.sqlx_test_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    );
    (&mut connection)
        .execute(AssertSqlSafe(format!(
            "CREATE TABLE {table} (id INTEGER, value VARCHAR(100), payload VARBINARY(2000))"
        )))
        .await
        .unwrap();
    let result: Result<(), sqlx_core::Error> = async {
        let mut transaction = connection.begin().await?;
        let inserted = sqlx_core::query::query::<sqlx_odbc::Odbc>(AssertSqlSafe(format!(
            "INSERT INTO {table} (id, value) VALUES (?, ?)"
        )))
        .bind(1i32)
        .bind("test")
        .execute(&mut *transaction)
        .await?;
        assert_eq!(inserted.rows_affected(), 1);
        for payload in [vec![0, 255, 0, 128], vec![42; 2000]] {
            sqlx_core::query::query::<sqlx_odbc::Odbc>(AssertSqlSafe(format!("INSERT INTO {table} (id, payload) VALUES (3, ?)"))).bind(payload.clone()).execute(&mut *transaction).await?;
            let row = (&mut *transaction).fetch_one(AssertSqlSafe(format!("SELECT payload FROM {table} WHERE id = 3"))).await?;
            assert_eq!(row.try_get::<Vec<u8>, _>(0)?, payload);
            (&mut *transaction).execute(AssertSqlSafe(format!("DELETE FROM {table} WHERE id = 3"))).await?;
        }
        transaction.rollback().await?;
        let row = (&mut connection)
            .fetch_one(AssertSqlSafe(format!(
                "SELECT CAST(COUNT(*) AS INTEGER) FROM {table}"
            )))
            .await?;
        assert_eq!(row.try_get::<i32, _>(0)?, 0);
        let result = (&mut connection)
            .execute(AssertSqlSafe(format!("DELETE FROM {table} WHERE id = 1")))
            .await?;
        assert_eq!(result.rows_affected(), 0);
        let error = (&mut connection).fetch_all(AssertSqlSafe(format!("INSERT INTO {table} (id, value) VALUES (2, 'batch'); SELECT id FROM {table}; DELETE FROM {table}"))).await.unwrap_err();
        let native = error.as_database_error().unwrap().downcast_ref::<sqlx_odbc::OdbcDatabaseError>();
        assert_eq!(native.diagnostics().unwrap().last().native_error, -210056);
        Ok(())
    }
    .await;
    let _ = connection.close().await;
    (&mut cleanup)
        .execute(AssertSqlSafe(format!("DROP TABLE {table}")))
        .await
        .unwrap();
    cleanup.close().await.unwrap();
    result.unwrap();
}

#[tokio::test]
#[ignore = "requires OpenEdge ODBC_TEST_CONNECTION_STRING and scratch-table privileges"]
async fn openedge_large_text_round_trips() {
    openedge_text_round_trips(&[
        String::new(),
        "café, Ångström, £, ÿ".to_owned(),
        "abcdefgh".repeat(2000),
        "éöñÿ".repeat(2000),
    ])
    .await;
}

#[tokio::test]
#[ignore = "requires Unicode-capable OpenEdge ODBC_TEST_CONNECTION_STRING and scratch-table privileges"]
async fn openedge_unicode_round_trips() {
    openedge_text_round_trips(&["Ελληνικά 日本語".to_owned()]).await;
}

async fn openedge_text_round_trips(values: &[String]) {
    use futures_util::FutureExt;
    use sqlx_core::sql_str::AssertSqlSafe;
    let options = OdbcConnectOptions::new(std::env::var("ODBC_TEST_CONNECTION_STRING").unwrap())
        .statement_cache_capacity(0);
    let mut setup = options.connect().await.unwrap();
    let table = format!(
        "PUB.sqlx_text_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    );
    (&mut setup)
        .execute(AssertSqlSafe(format!(
            "CREATE TABLE {table} (value VARCHAR(16000))"
        )))
        .await
        .unwrap();
    setup.close().await.unwrap();
    let result = std::panic::AssertUnwindSafe(async {
        for buffered in [false, true] {
            let mut connection = options.clone().buffered(buffered).connect().await?;
            let outcome = std::panic::AssertUnwindSafe(async {
                let mut transaction = connection.begin().await?;
                for value in values {
                    sqlx_core::query::query::<sqlx_odbc::Odbc>(AssertSqlSafe(format!(
                        "INSERT INTO {table} (value) VALUES (?)"
                    )))
                    .bind(value.clone())
                    .execute(&mut *transaction)
                    .await?;
                    let rows = (&mut *transaction)
                        .fetch_all(AssertSqlSafe(format!("SELECT value FROM {table}")))
                        .await?;
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0].try_get::<String, _>(0)?, *value);
                    (&mut *transaction)
                        .execute(AssertSqlSafe(format!("DELETE FROM {table}")))
                        .await?;
                }
                transaction.rollback().await?;
                Ok::<_, sqlx_core::Error>(())
            })
            .catch_unwind()
            .await;
            let closed = connection.close().await;
            if let Err(panic) = outcome {
                std::panic::resume_unwind(panic);
            }
            outcome.unwrap()?;
            closed?;
        }
        Ok::<_, sqlx_core::Error>(())
    })
    .catch_unwind()
    .await;
    let cleanup_result = async {
        let mut cleanup = options.connect().await?;
        let dropped = (&mut cleanup)
            .execute(AssertSqlSafe(format!("DROP TABLE {table}")))
            .await;
        let closed = cleanup.close().await;
        dropped?;
        closed
    }
    .await;
    if let Err(error) = &cleanup_result {
        eprintln!("scratch table {table} cleanup failed: {error}");
    }
    match result {
        Ok(result) => result.unwrap(),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    cleanup_result.unwrap();
}

#[tokio::test]
#[ignore = "requires OpenEdge ODBC_TEST_CONNECTION_STRING"]
async fn openedge_pool_recovers_after_task_cancellation() {
    let options = OdbcConnectOptions::new(std::env::var("ODBC_TEST_CONNECTION_STRING").unwrap());
    let pool = sqlx_odbc::OdbcPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    let borrower = pool.clone();
    let (ready, started) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let _transaction = borrower.begin().await.unwrap();
        ready.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    started.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let mut connection = tokio::time::timeout(std::time::Duration::from_secs(10), pool.acquire())
        .await
        .unwrap()
        .unwrap();
    assert!(!connection.is_in_transaction());
    connection.ping().await.unwrap();
    connection.begin().await.unwrap().rollback().await.unwrap();
    drop(connection);
    pool.close().await;
}

#[test]
#[ignore = "requires ODBC_TEST_CONNECTION_STRING and ODBC_TEST_QUERY returning one integer column"]
fn live_query_and_transaction_lifecycle() {
    let connection_string =
        std::env::var("ODBC_TEST_CONNECTION_STRING").expect("missing test connection string");
    let query = std::env::var("ODBC_TEST_QUERY").expect("missing read-only test query");
    futures_executor::block_on(async {
        let mut connection = OdbcConnectOptions::new(connection_string)
            .connect()
            .await
            .unwrap();
        connection.ping().await.unwrap();
        let sql = sqlx_core::sql_str::AssertSqlSafe(query);
        let rows = (&mut connection).fetch_all(sql).await.unwrap();
        assert!(!rows.is_empty());
        let _: i32 = rows[0].try_get(0).unwrap();
        let transaction = connection.begin().await.unwrap();
        drop(transaction);
        connection.ping().await.unwrap();
        connection.close().await.unwrap();
    });
}

#[test]
#[ignore = "requires an OpenEdge ODBC_TEST_CONNECTION_STRING"]
fn openedge_cache_limits_and_abandoned_streams() {
    use futures_util::TryStreamExt;
    let connection_string =
        std::env::var("ODBC_TEST_CONNECTION_STRING").expect("missing test connection string");
    futures_executor::block_on(async {
        for (capacity, buffered) in [(0, false), (1, false), (2, false), (2, true)] {
            let mut connection = OdbcConnectOptions::new(connection_string.clone())
                .statement_cache_capacity(capacity)
                .buffered(buffered)
                .connect()
                .await
                .unwrap();
            for sql in [
                "SELECT TOP 1 CAST(1 AS INTEGER) FROM SYSPROGRESS.SYSTABLES",
                "SELECT TOP 1 CAST(2 AS INTEGER) FROM SYSPROGRESS.SYSTABLES",
                "SELECT TOP 1 CAST(3 AS INTEGER) FROM SYSPROGRESS.SYSTABLES",
            ] {
                sqlx_core::query::query::<sqlx_odbc::Odbc>(sql)
                    .fetch_all(&mut connection)
                    .await
                    .unwrap();
                assert!(connection.cached_statements_size() <= capacity);
            }
            assert_eq!(connection.cached_statements_size(), capacity);
            let mut stream = sqlx_core::query::query::<sqlx_odbc::Odbc>(
                "SELECT CAST(1 AS INTEGER) FROM SYSPROGRESS.SYSTABLES",
            )
            .fetch(&mut connection);
            let retained = stream.try_next().await.unwrap().unwrap();
            drop(stream);
            connection.ping().await.unwrap();
            assert_eq!(retained.try_get::<i32, _>(0).unwrap(), 1);
            connection.clear_cached_statements().await.unwrap();
            assert_eq!(connection.cached_statements_size(), 0);
            connection.close().await.unwrap();
        }
    });
}

#[test]
#[ignore = "requires an OpenEdge ODBC_TEST_CONNECTION_STRING"]
fn openedge_server_overflow_retires_connection() {
    let connection_string =
        std::env::var("ODBC_TEST_CONNECTION_STRING").expect("missing test connection string");
    futures_executor::block_on(async {
        for buffered in [false, true] {
            let mut connection = OdbcConnectOptions::new(connection_string.clone())
                .buffered(buffered)
                .connect()
                .await
                .unwrap();
            let error = sqlx_core::query::query::<sqlx_odbc::Odbc>(
                "SELECT TOP 1 REPEAT('x', 40000) FROM SYSPROGRESS.SYSTABLES",
            )
            .fetch_all(&mut connection)
            .await
            .unwrap_err();
            let database = error.as_database_error().expect("expected server overflow");
            let native = database.downcast_ref::<sqlx_odbc::OdbcDatabaseError>();
            assert_eq!(native.diagnostics().unwrap().last().native_error, -20052);
            assert!(connection.ping().await.is_err());
        }
    });
}

#[test]
#[ignore = "requires an OpenEdge ODBC_TEST_CONNECTION_STRING"]
fn openedge_large_text_retrieval() {
    let connection_string = std::env::var("ODBC_TEST_CONNECTION_STRING").unwrap();
    futures_executor::block_on(async {
        for buffered in [false, true] {
            let mut connection = OdbcConnectOptions::new(connection_string.clone())
                .buffered(buffered)
                .connect()
                .await
                .unwrap();
            let rows = (&mut connection)
                .fetch_all("SELECT TOP 1 REPEAT('abcdefgh', 2000) FROM SYSPROGRESS.SYSTABLES")
                .await
                .unwrap();
            connection.close().await.unwrap();
            assert_eq!(
                rows[0].try_get::<String, _>(0).unwrap(),
                "abcdefgh".repeat(2000)
            );
        }
    });
}

#[test]
#[ignore = "requires an OpenEdge ODBC_TEST_CONNECTION_STRING"]
fn openedge_buffered_type_parity() {
    let connection_string =
        std::env::var("ODBC_TEST_CONNECTION_STRING").expect("missing test connection string");
    futures_executor::block_on(async {
        for buffered in [false, true] {
            let mut connection = OdbcConnectOptions::new(connection_string.clone())
                .buffered(buffered)
                .connect()
                .await
                .unwrap();
            let rows = sqlx_core::query::query::<sqlx_odbc::Odbc>("SELECT TOP 1 CAST(-123 AS SMALLINT), CAST(2147483647 AS INTEGER), CAST(1.25 AS DOUBLE PRECISION), CAST('hello' AS VARCHAR(20)), CAST(NULL AS VARCHAR(20)) FROM SYSPROGRESS.SYSTABLES")
                .fetch_all(&mut connection).await.unwrap();
            assert_eq!(rows[0].try_get::<i16, _>(0).unwrap(), -123);
            assert_eq!(rows[0].try_get::<i32, _>(1).unwrap(), i32::MAX);
            assert_eq!(rows[0].try_get::<f64, _>(2).unwrap(), 1.25);
            assert_eq!(rows[0].try_get::<String, _>(3).unwrap(), "hello");
            assert_eq!(rows[0].try_get::<Option<String>, _>(4).unwrap(), None);
            #[cfg(all(feature = "chrono", feature = "rust_decimal"))]
            {
                let rows = sqlx_core::query::query::<sqlx_odbc::Odbc>("SELECT TOP 1 CAST('2024-02-29' AS DATE), CAST('12:34:56' AS TIME), CAST('2024-02-29 12:34:56.123' AS TIMESTAMP), CAST(123.4500 AS DECIMAL(10,4)) FROM SYSPROGRESS.SYSTABLES")
                    .fetch_all(&mut connection).await.unwrap();
                assert_eq!(
                    rows[0]
                        .try_get::<chrono::NaiveDate, _>(0)
                        .unwrap()
                        .to_string(),
                    "2024-02-29"
                );
                assert_eq!(
                    rows[0]
                        .try_get::<chrono::NaiveTime, _>(1)
                        .unwrap()
                        .to_string(),
                    "12:34:56"
                );
                assert_eq!(
                    rows[0]
                        .try_get::<chrono::NaiveDateTime, _>(2)
                        .unwrap()
                        .to_string(),
                    "2024-02-29 12:34:56.123"
                );
                assert_eq!(
                    rows[0].try_get::<rust_decimal::Decimal, _>(3).unwrap(),
                    rust_decimal::Decimal::new(1234500, 4)
                );
            }
            connection.close().await.unwrap();
        }
    });
}

#[test]
#[ignore = "requires an OpenEdge ODBC_TEST_CONNECTION_STRING"]
fn openedge_parameters_and_metadata() {
    let connection_string =
        std::env::var("ODBC_TEST_CONNECTION_STRING").expect("missing test connection string");
    futures_executor::block_on(async {
        let mut connection = OdbcConnectOptions::new(connection_string)
            .connect()
            .await
            .unwrap();
        for value in [i32::MIN, 0, i32::MAX] {
            let row = sqlx_core::query::query::<sqlx_odbc::Odbc>("SELECT TOP 1 CAST(1 AS INTEGER) AS value FROM SYSPROGRESS.SYSTABLES WHERE CAST(? AS INTEGER) = CAST(? AS INTEGER)")
                .bind(value).bind(value).fetch_one(&mut connection).await.unwrap();
            assert_eq!(row.try_get::<i32, _>(0).unwrap(), 1);
        }
        let row = sqlx_core::query::query::<sqlx_odbc::Odbc>("SELECT TOP 1 CAST(NULL AS INTEGER) AS value FROM SYSPROGRESS.SYSTABLES WHERE CAST(? AS INTEGER) IS NULL")
            .bind(None::<i32>).fetch_one(&mut connection).await.unwrap();
        assert_eq!(row.try_get::<Option<i32>, _>(0).unwrap(), None);
        for value in ["", "hello", "Ελληνικά 日本語"] {
            let row = sqlx_core::query::query::<sqlx_odbc::Odbc>("SELECT TOP 1 CAST(1 AS INTEGER) AS value FROM SYSPROGRESS.SYSTABLES WHERE CAST(? AS VARCHAR(100)) = CAST(? AS VARCHAR(100))")
                .bind(value).bind(value).fetch_one(&mut connection).await.unwrap();
            assert_eq!(row.try_get::<i32, _>(0).unwrap(), 1);
        }
        use sqlx_core::sql_str::SqlSafeStr;
        let described = (&mut connection)
            .describe(
                "SELECT TOP 1 CAST(1 AS INTEGER) AS value FROM SYSPROGRESS.SYSTABLES"
                    .into_sql_str(),
            )
            .await
            .unwrap();
        assert_eq!(described.columns.len(), 1);
        connection.clear_cached_statements().await.unwrap();
        assert_eq!(connection.cached_statements_size(), 0);
        let sql = "SELECT TOP 1 CAST(1 AS INTEGER) FROM SYSPROGRESS.SYSTABLES";
        for _ in 0..3 {
            sqlx_core::query::query::<sqlx_odbc::Odbc>(sql)
                .fetch_all(&mut connection)
                .await
                .unwrap();
            assert_eq!(connection.cached_statements_size(), 1);
        }
        connection.clear_cached_statements().await.unwrap();
        sqlx_core::query::query::<sqlx_odbc::Odbc>(sql)
            .persistent(false)
            .fetch_all(&mut connection)
            .await
            .unwrap();
        assert_eq!(connection.cached_statements_size(), 0);
        connection.close().await.unwrap();
    });
}
