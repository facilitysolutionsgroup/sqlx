use super::*;
use futures_executor::block_on;
use sqlx_core::{
    arguments::Arguments,
    column::ColumnIndex,
    connection::ConnectOptions,
    encode::{Encode, IsNull},
    error::{BoxDynError, Error},
    row::Row,
    sql_str::SqlSafeStr,
    statement::Statement,
    types::Type,
    value::{Value, ValueRef},
};

struct TestText(&'static str);
impl Type<Odbc> for TestText {
    fn type_info() -> OdbcTypeInfo {
        OdbcTypeInfo("VARCHAR".into())
    }
}
impl Encode<'_, Odbc> for TestText {
    fn encode_by_ref(&self, buffer: &mut Vec<OdbcValue>) -> Result<IsNull, BoxDynError> {
        buffer.push(OdbcValue {
            type_info: Self::type_info(),
            data: Some(self.0.as_bytes().into()),
        });
        if self.0 == "fail" {
            return Err("test encoding failure".into());
        }
        Ok(IsNull::No)
    }
}

#[test]
fn arguments_preserve_typed_null_and_empty_values() {
    let mut arguments = OdbcArguments::default();
    arguments.add(None::<TestText>).unwrap();
    arguments.add(TestText("")).unwrap();
    assert_eq!(arguments.len(), 2);
    assert!(arguments.0[0].is_null());
    assert_eq!(arguments.0[0].type_info, TestText::type_info());
    assert!(!arguments.0[1].is_null());
    assert!(arguments.add(TestText("fail")).is_err());
    assert_eq!(arguments.len(), 2);
}

#[test]
fn options_round_trip_and_redact_secrets() {
    let options = OdbcConnectOptions::new("DSN=example;PWD={secret;&=};UID=user");
    let decoded = OdbcConnectOptions::from_url(&options.to_url_lossy()).unwrap();
    assert_eq!(decoded.connection_string(), options.connection_string());
    assert!(!format!("{options:?}").contains("secret"));
    assert!(block_on(OdbcConnectOptions::new("").connect()).is_err());
}

#[test]
fn invalid_urls_are_rejected_without_echoing_credentials() {
    for url in [
        "postgres://localhost/db",
        "odbc://host?connection_string=secret",
        "odbc://",
        "odbc://?connection_string=a&connection_string=secret",
        "odbc://?password=secret",
        "odbc://?connection_string=%00secret",
    ] {
        let error = url.parse::<OdbcConnectOptions>().unwrap_err();
        assert!(!error.to_string().contains("secret"));
    }
}

#[test]
fn rows_and_values_own_their_storage() {
    let row = OdbcRow {
        columns: vec![OdbcColumn {
            ordinal: 0,
            name: "value".into(),
            type_info: TestText::type_info(),
        }]
        .into(),
        values: vec![OdbcValue {
            type_info: TestText::type_info(),
            data: Some(b"retained".as_slice().into()),
        }]
        .into(),
    };
    assert!(matches!(
        row.try_get_raw(1),
        Err(Error::ColumnIndexOutOfBounds { .. })
    ));
    assert!(matches!(
        row.try_get_raw("missing"),
        Err(Error::ColumnNotFound(_))
    ));
    let owned = ValueRef::to_owned(&row.try_get_raw("value").unwrap());
    drop(row);
    assert_eq!(owned.data.as_deref(), Some(b"retained".as_slice()));
}

#[test]
fn statement_uses_current_owned_sql_contract() {
    let statement = OdbcStatement {
        sql: "SELECT ?".into_sql_str(),
        columns: vec![OdbcColumn {
            ordinal: 0,
            name: "value".into(),
            type_info: TestText::type_info(),
        }]
        .into(),
        parameters: None,
    };
    assert!(statement.parameters().is_none());
    assert_eq!("value".index(&statement).unwrap(), 0);
    let _query = statement.query().bind(TestText("test"));
    let _sql = statement.clone().into_sql();
}

#[test]
fn query_results_aggregate_without_wrapping() {
    let mut result = OdbcQueryResult::default();
    result.extend([
        OdbcQueryResult { rows_affected: 2 },
        OdbcQueryResult { rows_affected: 3 },
    ]);
    assert_eq!(result.rows_affected(), 5);
}

#[test]
fn standard_pool_and_query_contracts_compile() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<OdbcRow>();
    assert_send_sync::<OdbcStatement>();
    let _pool = OdbcPoolOptions::new();
    let _query = sqlx_core::query::query::<Odbc>("SELECT ?").bind(TestText("value"));
    fn executor<'c, T: OdbcExecutor<'c>>() {}
    executor::<&mut OdbcConnection>();
}
