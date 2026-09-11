use crate::Odbc;
#[cfg(any(feature = "chrono", feature = "rust_decimal"))]
use sqlx_core as sqlx;

sqlx_core::impl_type_checking!(
    Odbc { bool, i8, i16, i32, i64, f32, f64, String | &str, Vec<u8> | &[u8] },
    ParamChecking::Strong,
    feature-types: info => match info.0.as_ref() {
        "DATE" | "TIME" | "TIMESTAMP" => Some("chrono"),
        "DECIMAL" => Some("rust_decimal"),
        _ => None,
    },
    datetime-types: {
        chrono: { sqlx::types::chrono::NaiveDate, sqlx::types::chrono::NaiveTime, sqlx::types::chrono::NaiveDateTime },
        time: {},
    },
    numeric-types: {
        bigdecimal: {},
        rust_decimal: { sqlx::types::Decimal },
    },
);
