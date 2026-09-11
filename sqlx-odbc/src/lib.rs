mod buffered;
mod cache;
mod connection;
mod data;
#[cfg(feature = "rust_decimal")]
mod decimal;
mod error;
mod execution;
#[cfg(feature = "chrono")]
mod temporal;
#[cfg(feature = "offline")]
mod type_checking;
pub mod types;
mod worker;
pub use error::OdbcDatabaseError;
#[cfg(test)]
mod tests;

pub use connection::{OdbcConnectOptions, OdbcConnection, OdbcTransactionManager};
pub use data::{
    OdbcArguments, OdbcColumn, OdbcQueryResult, OdbcRow, OdbcStatement, OdbcTypeInfo, OdbcValue,
    OdbcValueRef,
};

#[derive(Debug, Clone, Copy)]
pub struct Odbc;
impl sqlx_core::database::HasStatementCache for Odbc {}

impl sqlx_core::database::Database for Odbc {
    type Connection = OdbcConnection;
    type TransactionManager = OdbcTransactionManager;
    type Row = OdbcRow;
    type QueryResult = OdbcQueryResult;
    type Column = OdbcColumn;
    type TypeInfo = OdbcTypeInfo;
    type Value = OdbcValue;
    type ValueRef<'r> = OdbcValueRef<'r>;
    type Arguments = OdbcArguments;
    type ArgumentBuffer = Vec<OdbcValue>;
    type Statement = OdbcStatement;
    const NAME: &'static str = "ODBC";
    const URL_SCHEMES: &'static [&'static str] = &["odbc"];
}

pub type OdbcPool = sqlx_core::pool::Pool<Odbc>;
pub type OdbcPoolOptions = sqlx_core::pool::PoolOptions<Odbc>;
pub type OdbcTransaction<'c> = sqlx_core::transaction::Transaction<'c, Odbc>;
pub trait OdbcExecutor<'c>: sqlx_core::executor::Executor<'c, Database = Odbc> {}
impl<'c, T: sqlx_core::executor::Executor<'c, Database = Odbc>> OdbcExecutor<'c> for T {}

sqlx_core::impl_encode_for_option!(Odbc);
sqlx_core::impl_into_arguments_for_arguments!(OdbcArguments);
sqlx_core::impl_acquire!(Odbc, OdbcConnection);

fn unavailable() -> sqlx_core::error::Error {
    sqlx_core::error::Error::Configuration(
        std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "ODBC native execution is not available in this driver scaffold",
        )
        .into(),
    )
}
