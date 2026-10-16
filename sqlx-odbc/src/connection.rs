use crate::{Odbc, OdbcQueryResult, OdbcRow, OdbcStatement, OdbcTypeInfo};
use futures_core::{future::BoxFuture, stream::BoxStream};
use sqlx_core::{
    connection::{ConnectOptions, Connection, LogSettings},
    describe::Describe,
    error::Error,
    executor::{Execute, Executor},
    sql_str::SqlStr,
    transaction::{Transaction, TransactionManager},
    Either,
};
use std::{fmt, str::FromStr, time::Duration};
use url::Url;

#[derive(Clone)]
pub struct OdbcConnectOptions {
    connection_string: String,
    log_settings: LogSettings,
    login_timeout_sec: Option<u32>,
    statement_cache_capacity: usize,
    buffered: bool,
}
impl fmt::Debug for OdbcConnectOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OdbcConnectOptions")
            .finish_non_exhaustive()
    }
}
impl OdbcConnectOptions {
    pub fn new(connection_string: impl Into<String>) -> Self {
        Self {
            connection_string: connection_string.into(),
            log_settings: LogSettings::default(),
            login_timeout_sec: Some(15),
            statement_cache_capacity: 100,
            buffered: false,
        }
    }
    pub fn buffered(mut self, enabled: bool) -> Self {
        self.buffered = enabled;
        self
    }
    pub fn statement_cache_capacity(mut self, capacity: usize) -> Self {
        self.statement_cache_capacity = capacity;
        self
    }
    pub fn login_timeout(mut self, seconds: u32) -> Self {
        self.login_timeout_sec = Some(seconds);
        self
    }
    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }
}
impl FromStr for OdbcConnectOptions {
    type Err = Error;
    fn from_str(value: &str) -> Result<Self, Error> {
        Self::from_url(
            &Url::parse(value).map_err(|_| Error::Configuration("invalid ODBC URL".into()))?,
        )
    }
}
impl ConnectOptions for OdbcConnectOptions {
    type Connection = OdbcConnection;
    fn from_url(url: &Url) -> Result<Self, Error> {
        if url.scheme() != "odbc"
            || url.host_str().is_some()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || !matches!(url.path(), "" | "/")
        {
            return Err(Error::Configuration(
                "expected odbc://?connection_string=<percent-encoded ODBC connection string>"
                    .into(),
            ));
        }
        let mut connection_string = None;
        for (key, value) in url.query_pairs() {
            if key != "connection_string" || connection_string.is_some() {
                return Err(Error::Configuration(
                    "unknown or duplicate ODBC URL option".into(),
                ));
            }
            connection_string = Some(value.into_owned());
        }
        let connection_string = connection_string
            .filter(|value| !value.is_empty() && !value.contains('\0'))
            .ok_or_else(|| {
                Error::Configuration("ODBC connection string is missing or invalid".into())
            })?;
        Ok(Self::new(connection_string))
    }
    fn to_url_lossy(&self) -> Url {
        let mut url = Url::parse("odbc://").expect("valid static ODBC URL");
        url.query_pairs_mut()
            .append_pair("connection_string", &self.connection_string);
        url
    }
    async fn connect(&self) -> Result<OdbcConnection, Error> {
        if self.connection_string.is_empty() || self.connection_string.contains('\0') {
            return Err(Error::Configuration(
                "ODBC connection string is missing or invalid".into(),
            ));
        }
        let worker = crate::worker::Worker::connect(
            self.connection_string.clone(),
            self.login_timeout_sec,
            self.statement_cache_capacity,
            self.buffered,
        )
        .await?;
        Ok(OdbcConnection {
            worker,
            transaction_depth: 0,
        })
    }
    fn log_statements(mut self, level: log::LevelFilter) -> Self {
        self.log_settings.log_statements(level);
        self
    }
    fn log_slow_statements(mut self, level: log::LevelFilter, duration: Duration) -> Self {
        self.log_settings.log_slow_statements(level, duration);
        self
    }
}

#[derive(Debug)]
pub struct OdbcConnection {
    worker: crate::worker::Worker,
    transaction_depth: usize,
}
impl Connection for OdbcConnection {
    type Database = Odbc;
    type Options = OdbcConnectOptions;
    async fn close(self) -> Result<(), Error> {
        self.worker.close().await
    }
    async fn close_hard(self) -> Result<(), Error> {
        self.worker.close().await
    }
    async fn ping(&mut self) -> Result<(), Error> {
        self.worker.ping().await
    }
    async fn begin(&mut self) -> Result<Transaction<'_, Odbc>, Error> {
        Transaction::begin(self, None).await
    }
    fn cached_statements_size(&self) -> usize {
        self.worker.cached_statements_size()
    }
    async fn clear_cached_statements(&mut self) -> Result<(), Error> {
        self.worker.clear_cached_statements().await
    }
    fn shrink_buffers(&mut self) {}
    async fn flush(&mut self) -> Result<(), Error> {
        self.worker.flush().await
    }
    fn should_flush(&self) -> bool {
        self.worker.should_flush()
    }
}

struct TransactionGuard<'a> {
    conn: &'a mut OdbcConnection,
    complete: bool,
}
impl TransactionGuard<'_> {
    fn finish(mut self) {
        self.complete = true;
    }
}
impl Drop for TransactionGuard<'_> {
    fn drop(&mut self) {
        if !self.complete {
            self.conn.worker.start_rollback();
            self.conn.transaction_depth = 0;
        }
    }
}
pub struct OdbcTransactionManager;
impl TransactionManager for OdbcTransactionManager {
    type Database = Odbc;
    async fn begin(conn: &mut OdbcConnection, statement: Option<SqlStr>) -> Result<(), Error> {
        if conn.transaction_depth != 0 {
            return Err(if statement.is_some() {
                Error::InvalidSavePointStatement
            } else {
                Error::Protocol("nested ODBC transactions are unsupported".into())
            });
        }
        if statement.is_some() {
            return Err(Error::Protocol(
                "custom ODBC begin statements are unsupported: transaction state cannot be verified"
                    .into(),
            ));
        }
        let guard = TransactionGuard {
            conn,
            complete: false,
        };
        guard.conn.worker.begin().await?;
        guard.conn.transaction_depth = 1;
        guard.finish();
        Ok(())
    }
    async fn commit(conn: &mut OdbcConnection) -> Result<(), Error> {
        if conn.transaction_depth == 0 {
            return Ok(());
        }
        let guard = TransactionGuard {
            conn,
            complete: false,
        };
        guard.conn.worker.commit().await?;
        guard.conn.transaction_depth = 0;
        guard.finish();
        Ok(())
    }
    async fn rollback(conn: &mut OdbcConnection) -> Result<(), Error> {
        if conn.transaction_depth == 0 {
            return Ok(());
        }
        Self::start_rollback(conn);
        conn.worker.flush().await
    }
    fn start_rollback(conn: &mut OdbcConnection) {
        if conn.transaction_depth != 0 {
            conn.worker.start_rollback();
            conn.transaction_depth = 0;
        }
    }
    fn get_transaction_depth(conn: &OdbcConnection) -> usize {
        conn.transaction_depth
    }
}
impl<'c> Executor<'c> for &'c mut OdbcConnection {
    type Database = Odbc;
    fn fetch_many<'e, 'q: 'e, E>(
        self,
        mut query: E,
    ) -> BoxStream<'e, Result<Either<OdbcQueryResult, OdbcRow>, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Odbc>,
    {
        use futures_util::TryStreamExt;
        Box::pin(
            futures_util::stream::once(async move {
                let arguments = query
                    .take_arguments()
                    .map_err(Error::Encode)?
                    .unwrap_or_default();
                let persistent = query.persistent();
                let sql = query.sql();
                let rows = self
                    .worker
                    .execute(sql.as_str().to_owned(), arguments, persistent)
                    .await?;
                Ok::<_, Error>(futures_util::stream::try_unfold(
                    (rows, self),
                    |(rows, connection)| async move {
                        match connection.worker.receive(&rows).await? {
                            Some(result) => Ok(Some((result?, (rows, connection)))),
                            None => {
                                connection.worker.flush().await?;
                                Ok(None)
                            }
                        }
                    },
                ))
            })
            .try_flatten(),
        )
    }
    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<OdbcRow>, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Odbc>,
    {
        use futures_util::TryStreamExt;
        Box::pin(async move {
            let mut rows = self.fetch_many(query);
            while let Some(result) = rows.try_next().await? {
                if let Either::Right(row) = result {
                    return Ok(Some(row));
                }
            }
            Ok(None)
        })
    }
    fn prepare_with<'e>(
        self,
        sql: SqlStr,
        _parameters: &'e [OdbcTypeInfo],
    ) -> BoxFuture<'e, Result<OdbcStatement, Error>>
    where
        'c: 'e,
    {
        Box::pin(async move {
            let described = self.worker.describe(sql.as_str().to_owned()).await?;
            let parameters = match described.parameters {
                Some(Either::Left(types)) => Some(types),
                _ => None,
            };
            Ok(OdbcStatement {
                sql,
                columns: described.columns.into(),
                parameters,
            })
        })
    }
    fn describe<'e>(self, sql: SqlStr) -> BoxFuture<'e, Result<Describe<Odbc>, Error>>
    where
        'c: 'e,
    {
        Box::pin(async move { self.worker.describe(sql.as_str().to_owned()).await })
    }
}

#[cfg(test)]
mod transaction_tests {
    use super::*;
    use futures_executor::block_on;
    struct Native(flume::Sender<&'static str>);
    impl crate::worker::NativeConnection for Native {
        fn begin(&mut self) -> Result<(), Error> {
            self.0.send("begin").unwrap();
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            self.0.send("commit").unwrap();
            Ok(())
        }
        fn rollback(&mut self) -> Result<(), Error> {
            self.0.send("rollback").unwrap();
            Ok(())
        }
        fn ping(&mut self) -> Result<(), Error> {
            self.0.send("ping").unwrap();
            Ok(())
        }
    }
    #[test]
    fn rejected_custom_begin_preserves_state_and_connection() {
        use sqlx_core::sql_str::SqlSafeStr;
        let (events, observed) = flume::unbounded();
        let worker = block_on(crate::worker::Worker::spawn(|| Ok(Native(events)))).unwrap();
        let mut connection = OdbcConnection {
            worker,
            transaction_depth: 0,
        };
        block_on(async {
            for statement in ["BEGIN", "SELECT 1", ""] {
                let error =
                    OdbcTransactionManager::begin(&mut connection, Some(statement.into_sql_str()))
                        .await
                        .unwrap_err();
                assert!(matches!(error, Error::Protocol(_)));
                assert!(!connection.is_in_transaction());
                assert!(!connection.should_flush());
            }
            OdbcTransactionManager::begin(&mut connection, None)
                .await
                .unwrap();
            let error = OdbcTransactionManager::begin(
                &mut connection,
                Some("SAVEPOINT custom".into_sql_str()),
            )
            .await
            .unwrap_err();
            assert!(matches!(error, Error::InvalidSavePointStatement));
            assert_eq!(connection.transaction_depth, 1);
            OdbcTransactionManager::rollback(&mut connection)
                .await
                .unwrap();
            connection.ping().await.unwrap();
            connection.close().await.unwrap();
        });
        assert_eq!(
            observed.drain().collect::<Vec<_>>(),
            ["begin", "rollback", "ping"]
        );
    }

    #[test]
    fn dropped_transaction_rolls_back_before_reuse() {
        let (events, observed) = flume::unbounded();
        let worker = block_on(crate::worker::Worker::spawn(|| Ok(Native(events)))).unwrap();
        let mut connection = OdbcConnection {
            worker,
            transaction_depth: 0,
        };
        block_on(async {
            let mut transaction = connection.begin().await.unwrap();
            assert!(transaction.begin().await.is_err());
            drop(transaction);
            connection.ping().await.unwrap();
            assert!(!connection.is_in_transaction());
            connection.begin().await.unwrap().commit().await.unwrap();
            connection.close().await.unwrap();
        });
        assert_eq!(
            observed.drain().collect::<Vec<_>>(),
            ["begin", "rollback", "ping", "begin", "commit"]
        );
    }
}
