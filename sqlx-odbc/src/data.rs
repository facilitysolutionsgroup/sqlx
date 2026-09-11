use crate::Odbc;
use sqlx_core::{
    arguments::Arguments,
    column::{Column, ColumnIndex},
    encode::{Encode, IsNull},
    error::{BoxDynError, Error},
    row::Row,
    sql_str::SqlStr,
    statement::Statement,
    type_info::TypeInfo,
    types::Type,
    value::{Value, ValueRef},
    Either,
};
use std::{borrow::Cow, fmt, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "offline", derive(serde::Serialize, serde::Deserialize))]
pub struct OdbcTypeInfo(pub(crate) Arc<str>);
impl TypeInfo for OdbcTypeInfo {
    fn is_null(&self) -> bool {
        self.0.as_ref() == "NULL"
    }
    fn name(&self) -> &str {
        &self.0
    }
}
impl fmt::Display for OdbcTypeInfo {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(Debug, Clone)]
pub struct OdbcValue {
    pub(crate) type_info: OdbcTypeInfo,
    pub(crate) data: Option<Arc<[u8]>>,
}
#[derive(Debug, Clone, Copy)]
pub struct OdbcValueRef<'r>(pub(crate) &'r OdbcValue);
impl Value for OdbcValue {
    type Database = Odbc;
    fn as_ref(&self) -> OdbcValueRef<'_> {
        OdbcValueRef(self)
    }
    fn type_info(&self) -> Cow<'_, OdbcTypeInfo> {
        Cow::Borrowed(&self.type_info)
    }
    fn is_null(&self) -> bool {
        self.data.is_none()
    }
}
impl<'r> ValueRef<'r> for OdbcValueRef<'r> {
    type Database = Odbc;
    fn to_owned(&self) -> OdbcValue {
        self.0.clone()
    }
    fn type_info(&self) -> Cow<'_, OdbcTypeInfo> {
        self.0.type_info()
    }
    fn is_null(&self) -> bool {
        self.0.is_null()
    }
}

#[derive(Debug, Default)]
pub struct OdbcArguments(pub(crate) Vec<OdbcValue>);
impl Arguments for OdbcArguments {
    type Database = Odbc;
    fn reserve(&mut self, additional: usize, _size: usize) {
        self.0.reserve(additional);
    }
    fn add<'t, T>(&mut self, value: T) -> Result<(), BoxDynError>
    where
        T: Encode<'t, Odbc> + Type<Odbc>,
    {
        let type_info = value.produces().unwrap_or_else(T::type_info);
        let mut encoded = Vec::new();
        if matches!(value.encode(&mut encoded)?, IsNull::Yes) {
            self.0.push(OdbcValue {
                type_info,
                data: None,
            });
        } else if encoded.len() == 1 {
            self.0.extend(encoded);
        } else {
            return Err("ODBC parameter encoding must produce exactly one value".into());
        }
        Ok(())
    }
    fn len(&self) -> usize {
        self.0.len()
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "offline", derive(serde::Serialize, serde::Deserialize))]
pub struct OdbcColumn {
    pub(crate) ordinal: usize,
    pub(crate) name: Arc<str>,
    pub(crate) type_info: OdbcTypeInfo,
}
impl Column for OdbcColumn {
    type Database = Odbc;
    fn ordinal(&self) -> usize {
        self.ordinal
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn type_info(&self) -> &OdbcTypeInfo {
        &self.type_info
    }
}

#[derive(Debug, Clone)]
pub struct OdbcRow {
    pub(crate) columns: Arc<[OdbcColumn]>,
    pub(crate) values: Arc<[OdbcValue]>,
}
impl Row for OdbcRow {
    type Database = Odbc;
    fn columns(&self) -> &[OdbcColumn] {
        &self.columns
    }
    fn try_get_raw<I: ColumnIndex<Self>>(&self, index: I) -> Result<OdbcValueRef<'_>, Error> {
        let index = index.index(self)?;
        self.values
            .get(index)
            .map(Value::as_ref)
            .ok_or(Error::ColumnIndexOutOfBounds {
                index,
                len: self.values.len(),
            })
    }
}
impl ColumnIndex<OdbcRow> for str {
    fn index(&self, row: &OdbcRow) -> Result<usize, Error> {
        row.columns
            .iter()
            .position(|column| column.name() == self)
            .ok_or_else(|| Error::ColumnNotFound(self.into()))
    }
}
sqlx_core::impl_column_index_for_row!(OdbcRow);

#[derive(Debug, Clone)]
pub struct OdbcStatement {
    pub(crate) sql: SqlStr,
    pub(crate) columns: Arc<[OdbcColumn]>,
    pub(crate) parameters: Option<Vec<OdbcTypeInfo>>,
}
impl Statement for OdbcStatement {
    type Database = Odbc;
    fn into_sql(self) -> SqlStr {
        self.sql
    }
    fn sql(&self) -> &SqlStr {
        &self.sql
    }
    fn parameters(&self) -> Option<Either<&[OdbcTypeInfo], usize>> {
        self.parameters.as_deref().map(Either::Left)
    }
    fn columns(&self) -> &[OdbcColumn] {
        &self.columns
    }
    sqlx_core::impl_statement_query!(OdbcArguments);
}
impl ColumnIndex<OdbcStatement> for str {
    fn index(&self, statement: &OdbcStatement) -> Result<usize, Error> {
        statement
            .columns
            .iter()
            .position(|column| column.name() == self)
            .ok_or_else(|| Error::ColumnNotFound(self.into()))
    }
}
sqlx_core::impl_column_index_for_statement!(OdbcStatement);

#[derive(Debug, Default, Clone)]
pub struct OdbcQueryResult {
    pub(crate) rows_affected: u64,
}
impl OdbcQueryResult {
    pub fn rows_affected(&self) -> u64 {
        self.rows_affected
    }
}
impl Extend<Self> for OdbcQueryResult {
    fn extend<T: IntoIterator<Item = Self>>(&mut self, iter: T) {
        for result in iter {
            self.rows_affected = self.rows_affected.saturating_add(result.rows_affected);
        }
    }
}
