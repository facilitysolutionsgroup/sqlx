fn main() {
    let _ = sqlx::query!("SELECT TOP 1 CAST(1 AS INTEGER) AS value FROM SYSPROGRESS.SYSTABLES", 1i32);
}
