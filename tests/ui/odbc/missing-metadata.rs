fn main() {
    let _ = sqlx::query!("SELECT CAST(? AS INTEGER) AS value", 1i32);
}
