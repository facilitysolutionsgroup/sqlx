use sqlx_core::error::{DatabaseError, Error, ErrorKind};
use std::{borrow::Cow, error::Error as StdError, fmt};

#[derive(Debug)]
pub struct OdbcDatabaseError {
    source: odbc_api::Error,
    message: String,
    code: Option<String>,
}
impl OdbcDatabaseError {
    pub fn diagnostics(&self) -> Option<&odbc_api::DiagnosticRecords> {
        match &self.source {
            odbc_api::Error::Diagnostics { records, .. } => Some(records),
            _ => None,
        }
    }
    pub fn native_error(&self) -> &odbc_api::Error {
        &self.source
    }
}
impl fmt::Display for OdbcDatabaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}
impl StdError for OdbcDatabaseError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.source)
    }
}
impl DatabaseError for OdbcDatabaseError {
    fn message(&self) -> &str {
        &self.message
    }
    fn code(&self) -> Option<Cow<'_, str>> {
        self.code.as_deref().map(Cow::Borrowed)
    }
    fn as_error(&self) -> &(dyn StdError + Send + Sync + 'static) {
        self
    }
    fn as_error_mut(&mut self) -> &mut (dyn StdError + Send + Sync + 'static) {
        self
    }
    fn into_error(self: Box<Self>) -> Box<dyn StdError + Send + Sync + 'static> {
        self
    }
    fn kind(&self) -> ErrorKind {
        ErrorKind::Other
    }
}
pub(crate) fn map_error(source: odbc_api::Error) -> Error {
    let (message, code) = match &source {
        odbc_api::Error::Diagnostics { records, function } => {
            let messages: Vec<_> = records
                .iter()
                .map(|record| {
                    let text =
                        odbc_api::handles::slice_to_utf8(&record.message).unwrap_or_else(|_| {
                            format!("Invalid diagnostic encoding: {:?}", record.message)
                        });
                    format!(
                        "{} ({}): {}",
                        record.state.as_str(),
                        record.native_error,
                        text
                    )
                })
                .collect();
            (
                format!("{function}: {}", messages.join("\n")),
                Some(records.last().state.as_str().to_owned()),
            )
        }
        _ => return Error::Configuration(Box::new(source)),
    };
    Error::Database(Box::new(OdbcDatabaseError {
        source,
        message,
        code,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn diagnostics_survive_mapping_and_downcasting() {
        let record = |state, native_error, text: &str| odbc_api::handles::Record {
            state: odbc_api::handles::State(state),
            native_error,
            message: text
                .chars()
                .map(|character| character as odbc_api::handles::SqlChar)
                .collect(),
        };
        let error = map_error(odbc_api::Error::Diagnostics {
            records: odbc_api::DiagnosticRecords::new(
                record(*b"HY000", 1, "manager"),
                vec![record(*b"08001", 2, "driver")],
            ),
            function: "SQLDriverConnect",
        });
        let database = error.as_database_error().unwrap();
        assert_eq!(database.code().as_deref(), Some("08001"));
        assert!(database.message().contains("manager"));
        assert!(database.message().contains("driver"));
        let native = database.downcast_ref::<OdbcDatabaseError>();
        assert_eq!(native.diagnostics().unwrap().iter().count(), 2);
        assert!(native.source().is_some());
    }
    #[test]
    fn errors_without_diagnostics_preserve_source() {
        let error = map_error(odbc_api::Error::NoDiagnostics {
            function: "SQLConnect",
        });
        let Error::Configuration(source) = error else {
            panic!("expected native source")
        };
        assert!(source.downcast_ref::<odbc_api::Error>().is_some());
    }
}
