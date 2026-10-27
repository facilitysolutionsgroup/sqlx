use crate::{Odbc, OdbcArguments, OdbcTypeInfo, OdbcValue};
use sqlx_core::{
    encode::{Encode, IsNull},
    error::{BoxDynError, Error},
    types::Type,
    Either,
};
use std::fmt::{self, Display, Formatter};

/// ODBC type name reported for [`SqlInt`] values.
pub(crate) const LITERAL_TYPE: &str = "LITERAL";

/// An integer value spliced into the statement text as a literal.
///
/// Some data sources accept only literal integers in clauses such as `OFFSET` and `FETCH`,
/// rejecting parameter markers there with a server syntax error. Queries that use literal
/// markers (`{:name}`) in checked macros therefore render the bound values into the statement
/// text before it is prepared, and this newtype is the only value the generated code will
/// splice. It deliberately has conversions only from integer types: nothing string-like can
/// be converted, so the spliced text is always a decimal integer and cannot carry SQL syntax.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SqlInt {
    value: i64,
}

impl SqlInt {
    #[must_use]
    pub const fn new(value: i64) -> Self {
        Self { value }
    }

    #[must_use]
    pub const fn value(self) -> i64 {
        self.value
    }
}

impl Type<Odbc> for SqlInt {
    fn type_info() -> OdbcTypeInfo {
        OdbcTypeInfo(LITERAL_TYPE.into())
    }
}

impl Encode<'_, Odbc> for SqlInt {
    fn encode_by_ref(&self, buffer: &mut Vec<OdbcValue>) -> Result<IsNull, BoxDynError> {
        buffer.push(OdbcValue {
            type_info: <Self as Type<Odbc>>::type_info(),
            data: Some(self.value.to_string().into_bytes().into()),
        });
        Ok(IsNull::No)
    }
}

impl sqlx_core::decode::Decode<'_, Odbc> for SqlInt {
    fn decode(_value: crate::OdbcValueRef<'_>) -> Result<Self, BoxDynError> {
        Err("SqlInt is spliced into statement text and never read from the database".into())
    }
}

macro_rules! impl_sql_int_from {
    ($($source:ty),* $(,)?) => {
        $(
            impl From<$source> for SqlInt {
                fn from(value: $source) -> Self {
                    Self {
                        value: i64::from(value),
                    }
                }
            }

            impl From<&$source> for SqlInt {
                fn from(value: &$source) -> Self {
                    Self::from(*value)
                }
            }
        )*
    };
}

impl_sql_int_from!(i8, i16, i32, i64, u8, u16, u32);

/// How a parameter-like token of the statement text relates to the bind arguments.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ParamKind {
    Bind,
    Literal,
}

/// The parameter-like tokens of a statement with the byte spans of its literal markers.
struct Markers {
    kinds: Vec<ParamKind>,
    spans: Vec<(usize, usize)>,
}

/// Finds the parameter-like tokens (`?` and `{:name}`) of `sql`, outside strings and comments.
fn scan(sql: &str) -> Result<Option<Markers>, Error> {
    let mut kinds = Vec::new();
    let mut spans = Vec::new();
    let bytes = sql.as_bytes();
    let mut index = 0_usize;

    while index < bytes.len() {
        match bytes[index] {
            b'\'' | b'"' => {
                let quote = bytes[index];
                index += 1;
                while index < bytes.len() && bytes[index] != quote {
                    index += 1;
                }
                if index == bytes.len() {
                    return Err(protocol(format!(
                        "unterminated {:?}-quoted literal in statement",
                        quote as char
                    )));
                }
                index += 1;
            }
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
                {
                    index += 1;
                }
                if index + 1 >= bytes.len() {
                    return Err(protocol("unterminated /* comment in statement"));
                }
                index += 2;
            }
            b'?' => {
                kinds.push(ParamKind::Bind);
                index += 1;
            }
            b'{' if bytes.get(index + 1) == Some(&b':') => {
                let mut end = index + 2;
                while end < bytes.len()
                    && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_')
                {
                    end += 1;
                }
                if end >= bytes.len() || bytes[end] != b'}' {
                    return Err(protocol(
                        "literal marker {:...} is missing its closing brace",
                    ));
                }
                spans.push((index, end + 1));
                kinds.push(ParamKind::Literal);
                index = end + 1;
            }
            _ => index += 1,
        }
    }

    if kinds.is_empty() || spans.is_empty() {
        return Ok(None);
    }
    Ok(Some(Markers { kinds, spans }))
}

/// The statement with every literal marker replaced with `0`, used for describing; the
/// metadata does not depend on the value.
pub(crate) fn describe_sql(sql: &str) -> Result<Option<String>, Error> {
    let Some(markers) = scan(sql)? else {
        return Ok(None);
    };
    let mut described = String::with_capacity(sql.len());
    let mut position = 0;
    for (start, end) in markers.spans {
        described.push_str(&sql[position..start]);
        described.push('0');
        position = end;
    }
    described.push_str(&sql[position..]);
    Ok(Some(described))
}

/// Adds the [`LITERAL_TYPE`] parameter entries for the literal markers of `sql` to the
/// described bind parameters, so the macro sees one parameter per argument.
pub(crate) fn describe_parameters(
    parameters: Option<Either<Vec<OdbcTypeInfo>, usize>>,
    sql: &str,
) -> Result<Option<Either<Vec<OdbcTypeInfo>, usize>>, Error> {
    let Some(markers) = scan(sql)? else {
        return Ok(parameters);
    };
    let Some(bind_count) = markers.kinds.len().checked_sub(markers.spans.len()) else {
        return Err(protocol("more literal markers than parameter-like tokens"));
    };
    match parameters {
        Some(Either::Left(bind_types)) => {
            if bind_types.len() != bind_count {
                return Err(protocol(format!(
                    "statement describes {} bind parameters for {} bind markers",
                    bind_types.len(),
                    bind_count
                )));
            }
            let mut bind_types = bind_types.into_iter();
            Ok(Some(Either::Left(
                markers
                    .kinds
                    .iter()
                    .map(|kind| match kind {
                        ParamKind::Bind => bind_types.next().expect("bind count checked above"),
                        ParamKind::Literal => <SqlInt as Type<Odbc>>::type_info(),
                    })
                    .collect(),
            )))
        }
        _ => Err(protocol(
            "literal markers require described parameter types, which the driver did not provide",
        )),
    }
}

/// Removes the [`SqlInt`] arguments from `arguments` and returns their values in marker order.
pub(crate) fn take_literal_arguments(arguments: &mut OdbcArguments) -> Result<Vec<String>, Error> {
    let mut literals = Vec::new();
    let mut binds = Vec::with_capacity(arguments.0.len());
    for value in arguments.0.drain(..) {
        if value.type_info.0.as_ref() == LITERAL_TYPE {
            let data = value
                .data
                .ok_or_else(|| protocol("SqlInt arguments cannot be null"))?;
            literals.push(
                std::str::from_utf8(&data)
                    .map_err(|_| protocol("SqlInt arguments must be decimal integers"))?
                    .to_owned(),
            );
        } else {
            binds.push(value);
        }
    }
    arguments.0 = binds;
    Ok(literals)
}

/// The statement with the literal arguments spliced into the literal markers of `sql`.
pub(crate) fn substitute(sql: &str, literals: &[String]) -> Result<String, Error> {
    let Some(markers) = scan(sql)? else {
        if literals.is_empty() {
            return Ok(sql.to_owned());
        }
        return Err(protocol(
            "SqlInt arguments were given for a statement without literal markers",
        ));
    };
    if literals.len() != markers.spans.len() {
        return Err(protocol(format!(
            "statement has {} literal markers but {} SqlInt arguments were given",
            markers.spans.len(),
            literals.len()
        )));
    }
    let mut substituted = String::with_capacity(sql.len() + 11 * literals.len());
    let mut position = 0;
    for ((start, end), literal) in markers.spans.iter().copied().zip(literals) {
        substituted.push_str(&sql[position..start]);
        substituted.push_str(literal);
        position = end;
    }
    substituted.push_str(&sql[position..]);
    Ok(substituted)
}

fn protocol(message: impl Into<String>) -> Error {
    Error::Protocol(message.into())
}

impl Display for SqlInt {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.value)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        describe_parameters, describe_sql, substitute, take_literal_arguments, SqlInt, LITERAL_TYPE,
    };
    use crate::{OdbcArguments, OdbcTypeInfo};
    use sqlx_core::{arguments::Arguments, Either};

    #[test]
    fn encodes_integer_values_as_decimal_text() {
        let mut arguments = OdbcArguments::default();
        arguments.add(SqlInt::from(5001_i64)).unwrap();
        let value = &arguments.0[0];
        assert_eq!(value.type_info.0.as_ref(), LITERAL_TYPE);
        assert_eq!(
            std::str::from_utf8(value.data.as_ref().unwrap()).unwrap(),
            "5001"
        );
    }

    #[test]
    fn replaces_markers_with_dummy_literals_for_describing() {
        let sql = "SELECT * FROM t WHERE a = ? OFFSET {:offset} ROWS FETCH NEXT {:limit} ROWS ONLY";
        assert_eq!(
            describe_sql(sql).unwrap().unwrap(),
            "SELECT * FROM t WHERE a = ? OFFSET 0 ROWS FETCH NEXT 0 ROWS ONLY"
        );
        assert!(describe_sql("SELECT * FROM t WHERE a = ?")
            .unwrap()
            .is_none());
    }

    #[test]
    fn ignores_marks_inside_strings_comments_and_escapes() {
        let sql =
            "-- ? {:x}\nSELECT '{:not_a_marker} ?' /* {:y} */ FROM t WHERE a = ? OFFSET {:off}";
        assert_eq!(
            describe_sql(sql).unwrap().unwrap(),
            "-- ? {:x}\nSELECT '{:not_a_marker} ?' /* {:y} */ FROM t WHERE a = ? OFFSET 0"
        );
    }

    #[test]
    fn adds_literal_entries_to_described_bind_parameters() {
        let sql = "SELECT * FROM t WHERE a = ? OFFSET {:offset} ROWS";
        let bind = OdbcTypeInfo("VARCHAR".into());
        match describe_parameters(Some(Either::Left(vec![bind])), sql)
            .unwrap()
            .unwrap()
        {
            Either::Left(types) => {
                assert_eq!(types.len(), 2);
                assert_eq!(types[0].0.as_ref(), "VARCHAR");
                assert_eq!(types[1].0.as_ref(), LITERAL_TYPE);
            }
            Either::Right(_) => panic!("expected described parameter types"),
        }
    }

    #[test]
    fn separates_literal_arguments_from_binds() {
        let mut arguments = OdbcArguments::default();
        arguments.add("bind").unwrap();
        arguments.add(SqlInt::from(26_i64)).unwrap();
        arguments.add(SqlInt::from(100_i64)).unwrap();
        let literals = take_literal_arguments(&mut arguments).unwrap();
        assert_eq!(literals, ["26", "100"]);
        assert_eq!(arguments.0.len(), 1);
        assert_eq!(arguments.0[0].type_info.0.as_ref(), "VARCHAR");
    }

    #[test]
    fn splices_literal_arguments_into_markers() {
        let sql = "SELECT * FROM t WHERE a = ? OFFSET {:offset} ROWS FETCH NEXT {:limit} ROWS ONLY";
        let literals = ["100".to_owned(), "26".to_owned()];
        assert_eq!(
            substitute(sql, &literals).unwrap(),
            "SELECT * FROM t WHERE a = ? OFFSET 100 ROWS FETCH NEXT 26 ROWS ONLY"
        );
        assert!(substitute(sql, &literals[..1]).is_err());
        assert!(substitute("SELECT 1", &literals).is_err());
    }

    #[test]
    fn rejects_mismatched_bind_and_marker_counts_at_describe() {
        let sql = "SELECT * FROM t WHERE a = ? OFFSET {:offset} ROWS";
        assert!(describe_parameters(Some(Either::Left(vec![])), sql).is_err());
        assert!(describe_parameters(None, sql).is_err());
    }
}
