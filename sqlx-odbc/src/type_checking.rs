use crate::Odbc;

// The paths used below will also be emitted by the macros so they have to match the final
// facade.
#[allow(unused_imports, dead_code)]
mod sqlx {
    pub use crate as odbc;
    pub use sqlx_core::*;
}

sqlx_core::impl_type_checking!(
    Odbc { bool, i8, i16, i32, i64, f32, f64, String | &str, Vec<u8> | &[u8], sqlx::odbc::SqlInt },
    ParamChecking::Strong,
    feature-types: info => match info.0.as_ref() {
        "DATE" | "TIME" | "TIMESTAMP" => Some("chrono"),
        "DECIMAL" => Some("rust_decimal"),
        _ => None,
    },
    numeric-priority: |info: &crate::OdbcTypeInfo| info.0.as_ref() == "DECIMAL",
    datetime-types: {
        chrono: { sqlx::types::chrono::NaiveDate, sqlx::types::chrono::NaiveTime, sqlx::types::chrono::NaiveDateTime },
        time: {},
    },
    numeric-types: {
        bigdecimal: {},
        rust_decimal: { sqlx::types::Decimal },
    },
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OdbcTypeInfo;
    use sqlx_core::{
        config::macros::{NumericCrate, PreferredCrates},
        type_checking::{Error, TypeChecking},
    };

    #[test]
    fn decimal_inference_is_exact() {
        let preferred = PreferredCrates::default();
        let decimal = OdbcTypeInfo("DECIMAL".into());
        if cfg!(feature = "rust_decimal") {
            assert!(matches!(
                Odbc::param_type_for_id(&decimal, &preferred),
                Ok("sqlx::types::Decimal")
            ));
            assert!(matches!(
                Odbc::return_type_for_id(&decimal, &preferred),
                Ok("sqlx::types::Decimal")
            ));
        } else {
            assert!(matches!(
                Odbc::param_type_for_id(&decimal, &preferred),
                Err(Error::NoMappingFound)
            ));
            assert!(matches!(
                Odbc::return_type_for_id(&decimal, &preferred),
                Err(Error::NoMappingFound)
            ));
        }

        let disabled = PreferredCrates {
            numeric: NumericCrate::BigDecimal,
            ..Default::default()
        };
        assert!(matches!(
            Odbc::param_type_for_id(&decimal, &disabled),
            Err(Error::NumericCrateFeatureNotEnabled)
        ));
        assert!(matches!(
            Odbc::return_type_for_id(&decimal, &disabled),
            Err(Error::NumericCrateFeatureNotEnabled)
        ));
    }

    #[test]
    fn floating_point_inference_is_unchanged() {
        let preferred = PreferredCrates::default();
        for (name, expected) in [("REAL", "f32"), ("DOUBLE", "f64")] {
            let info = OdbcTypeInfo(name.into());
            assert_eq!(
                Odbc::param_type_for_id(&info, &preferred).ok(),
                Some(expected)
            );
            assert_eq!(
                Odbc::return_type_for_id(&info, &preferred).ok(),
                Some(expected)
            );
        }
    }
}
