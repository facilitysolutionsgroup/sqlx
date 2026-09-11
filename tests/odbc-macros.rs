#[test]
fn rejects_extra_arguments() {
    trybuild::TestCases::new().compile_fail("tests/ui/odbc/*.rs");
}

#[test]
fn checked_odbc_query_compiles() {
    let _query = sqlx::query!("SELECT TOP 1 CAST(1 AS INTEGER) AS value FROM SYSPROGRESS.SYSTABLES");
}
