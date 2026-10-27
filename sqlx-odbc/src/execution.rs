use crate::{error::map_error, OdbcColumn, OdbcQueryResult, OdbcRow, OdbcTypeInfo, OdbcValue};
use odbc_api::{Cursor, DataType, Nullable, ResultSetMetadata};
use sqlx_core::{error::Error, Either};
use std::sync::Arc;

pub(crate) type Output = Result<Either<OdbcQueryResult, OdbcRow>, Error>;
pub(crate) fn describe(
    connection: &odbc_api::Connection<'_>,
    sql: &str,
) -> Result<sqlx_core::describe::Describe<crate::Odbc>, Error> {
    // Literal markers are not bindable parameters, so describing replaces them with a dummy
    // literal; the metadata does not depend on the value.
    let described_sql = crate::literal::describe_sql(sql)?;
    let mut statement =
        connection.prepare(described_sql.as_deref().unwrap_or(sql))
            .map_err(map_error)?;
    let columns = columns(&mut statement)?.to_vec();
    let nullable = (1..=columns.len())
        .map(|index| {
            let index = u16::try_from(index).map_err(protocol)?;
            statement
                .col_nullability(index)
                .map_err(map_error)
                .map(|nullable| match nullable {
                    odbc_api::Nullability::Nullable => Some(true),
                    odbc_api::Nullability::NoNulls => Some(false),
                    _ => None,
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let count = statement.num_params().map_err(map_error)?;
    let parameters = (1..=count)
        .map(|index| {
            statement
                .describe_param(index)
                .map_err(map_error)
                .and_then(|parameter| {
                    type_name(parameter.data_type).map(|name| OdbcTypeInfo(name.into()))
                })
        })
        .collect::<Result<Vec<_>, _>>();
    let parameters = Some(match parameters {
        Ok(types) => Either::Left(types),
        Err(_) => Either::Right(usize::from(count)),
    });
    let parameters = crate::literal::describe_parameters(parameters, sql)?;
    Ok(sqlx_core::describe::Describe {
        columns,
        parameters,
        nullable,
    })
}
fn protocol(error: impl std::fmt::Display) -> Error {
    Error::Protocol(error.to_string())
}
fn type_name(data_type: DataType) -> Result<&'static str, Error> {
    Ok(match data_type {
        DataType::TinyInt => "TINYINT",
        DataType::SmallInt => "SMALLINT",
        DataType::Integer => "INTEGER",
        DataType::BigInt => "BIGINT",
        DataType::Real => "REAL",
        DataType::Float { .. } | DataType::Double => "DOUBLE",
        DataType::Bit => "BIT",
        DataType::Char { .. }
        | DataType::Varchar { .. }
        | DataType::LongVarchar { .. }
        | DataType::WChar { .. }
        | DataType::WVarchar { .. }
        | DataType::WLongVarchar { .. } => "VARCHAR",
        DataType::Binary { .. } | DataType::Varbinary { .. } | DataType::LongVarbinary { .. } => {
            "VARBINARY"
        }
        DataType::Numeric { .. } | DataType::Decimal { .. } => "DECIMAL",
        DataType::Date => "DATE",
        DataType::Time { .. } => "TIME",
        DataType::Timestamp { .. } => "TIMESTAMP",
        _ => return Err(protocol("unsupported ODBC result type")),
    })
}
pub(crate) fn columns(metadata: &mut impl ResultSetMetadata) -> Result<Arc<[OdbcColumn]>, Error> {
    let count = u16::try_from(metadata.num_result_cols().map_err(map_error)?).map_err(protocol)?;
    (1..=count)
        .map(|index| {
            let mut description = odbc_api::ColumnDescription::default();
            metadata
                .describe_col(index, &mut description)
                .map_err(map_error)?;
            Ok(OdbcColumn {
                ordinal: usize::from(index - 1),
                name: description.name_to_string().map_err(protocol)?.into(),
                type_info: OdbcTypeInfo(type_name(description.data_type)?.into()),
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Into::into)
}
fn row(cursor: &mut odbc_api::CursorRow<'_>, columns: Arc<[OdbcColumn]>) -> Result<OdbcRow, Error> {
    let mut values = Vec::with_capacity(columns.len());
    for column in columns.iter() {
        let index = u16::try_from(column.ordinal + 1).map_err(protocol)?;
        macro_rules! scalar {
            ($rust:ty) => {{
                let mut value = Nullable::<$rust>::null();
                cursor.get_data(index, &mut value).map_err(map_error)?;
                value
                    .as_opt()
                    .map(|value| Arc::<[u8]>::from(value.to_le_bytes().as_slice()))
            }};
        }
        let data = match column.type_info.0.as_ref() {
            "TINYINT" => scalar!(i8),
            "SMALLINT" => scalar!(i16),
            "INTEGER" => scalar!(i32),
            "BIGINT" => scalar!(i64),
            "REAL" => scalar!(f32),
            "DOUBLE" => scalar!(f64),
            "BIT" => {
                let mut value = Nullable::<odbc_api::Bit>::null();
                cursor.get_data(index, &mut value).map_err(map_error)?;
                value.as_opt().map(|value| Arc::<[u8]>::from([value.0]))
            }
            "VARBINARY" => {
                let mut data = Vec::new();
                cursor
                    .get_binary(index, &mut data)
                    .map_err(map_error)?
                    .then(|| data.into())
            }
            _ => {
                let mut text = Vec::new();
                if cursor.get_wide_text(index, &mut text).map_err(map_error)? {
                    Some(
                        String::from_utf16(&text)
                            .map_err(protocol)?
                            .into_bytes()
                            .into(),
                    )
                } else {
                    None
                }
            }
        };
        values.push(OdbcValue {
            type_info: column.type_info.clone(),
            data,
        });
    }
    Ok(OdbcRow {
        columns,
        values: values.into(),
    })
}
pub(crate) fn execute<S: odbc_api::handles::AsStatementRef>(
    prepared: &mut odbc_api::Prepared<S>,
    parameters: &[Box<dyn odbc_api::parameter::InputParameter>],
    output: &flume::Sender<Output>,
    buffered: bool,
) -> Result<(), Error> {
    prepared.set_query_timeout_sec(30).map_err(map_error)?;
    let mut had_cursor = false;
    if let Some(mut cursor) = prepared.execute(parameters).map_err(map_error)? {
        had_cursor = true;
        loop {
            let columns = columns(&mut cursor)?;
            if columns.is_empty() {
                use odbc_api::handles::{AsStatementRef, Statement};
                let mut statement = cursor.as_stmt_ref();
                let count = statement
                    .row_count()
                    .into_result(&statement)
                    .map_err(map_error)?;
                let rows_affected = u64::try_from(count).unwrap_or(0);
                if output
                    .send(Ok(Either::Left(OdbcQueryResult { rows_affected })))
                    .is_err()
                {
                    return Ok(());
                }
            } else if buffered {
                cursor = crate::buffered::fetch(cursor, columns, output)?;
                if output
                    .send(Ok(Either::Left(OdbcQueryResult::default())))
                    .is_err()
                {
                    return Ok(());
                }
            } else {
                while let Some(mut current) = cursor.next_row().map_err(map_error)? {
                    let row = row(&mut current, columns.clone())?;
                    if output.send(Ok(Either::Right(row))).is_err() {
                        return Ok(());
                    }
                }
                if output
                    .send(Ok(Either::Left(OdbcQueryResult::default())))
                    .is_err()
                {
                    return Ok(());
                }
            }
            match cursor.more_results().map_err(map_error)? {
                Some(next) => cursor = next,
                None => break,
            }
        }
    }
    if !had_cursor {
        use odbc_api::handles::{AsStatementRef, Statement};
        let mut statement = prepared.as_stmt_ref();
        loop {
            let count = statement
                .row_count()
                .into_result(&statement)
                .map_err(map_error)?;
            let rows_affected = u64::try_from(count).unwrap_or(0);
            if output
                .send(Ok(Either::Left(OdbcQueryResult { rows_affected })))
                .is_err()
            {
                return Ok(());
            }
            if !unsafe { statement.more_results() }
                .into_result_bool(&statement)
                .map_err(map_error)?
            {
                break;
            }
            if statement
                .num_result_cols()
                .into_result(&statement)
                .map_err(map_error)?
                != 0
            {
                let mut cursor = unsafe { odbc_api::CursorImpl::new(statement) };
                loop {
                    let columns = columns(&mut cursor)?;
                    if columns.is_empty() {
                        let mut handle = cursor.as_stmt_ref();
                        let count = handle.row_count().into_result(&handle).map_err(map_error)?;
                        let rows_affected = u64::try_from(count).unwrap_or(0);
                        if output
                            .send(Ok(Either::Left(OdbcQueryResult { rows_affected })))
                            .is_err()
                        {
                            return Ok(());
                        }
                    } else if buffered {
                        cursor = crate::buffered::fetch(cursor, columns, output)?;
                        if output
                            .send(Ok(Either::Left(OdbcQueryResult::default())))
                            .is_err()
                        {
                            return Ok(());
                        }
                    } else {
                        while let Some(mut current) = cursor.next_row().map_err(map_error)? {
                            if output
                                .send(Ok(Either::Right(row(&mut current, columns.clone())?)))
                                .is_err()
                            {
                                return Ok(());
                            }
                        }
                        if output
                            .send(Ok(Either::Left(OdbcQueryResult::default())))
                            .is_err()
                        {
                            return Ok(());
                        }
                    }
                    match cursor.more_results().map_err(map_error)? {
                        Some(next) => cursor = next,
                        None => return Ok(()),
                    }
                }
            }
        }
    }
    Ok(())
}
