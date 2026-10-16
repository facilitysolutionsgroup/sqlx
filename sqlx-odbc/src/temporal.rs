use crate::{Odbc, OdbcTypeInfo, OdbcValue, OdbcValueRef};
use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use odbc_api::{
    parameter::{InputParameter, WithDataType},
    DataType, Nullable,
};
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};

macro_rules! temporal {
    ($rust:ty, $name:literal, $format:literal) => {
        impl Type<Odbc> for $rust {
            fn type_info() -> OdbcTypeInfo {
                OdbcTypeInfo($name.into())
            }
        }
        impl Encode<'_, Odbc> for $rust {
            fn encode_by_ref(&self, buffer: &mut Vec<OdbcValue>) -> Result<IsNull, BoxDynError> {
                let text = self.format($format).to_string();
                let value = OdbcValue {
                    type_info: Self::type_info(),
                    data: Some(text.as_bytes().into()),
                };
                native(&value)?;
                buffer.push(value);
                Ok(IsNull::No)
            }
        }
        impl Decode<'_, Odbc> for $rust {
            fn decode(value: OdbcValueRef<'_>) -> Result<Self, BoxDynError> {
                let text = <&str as Decode<Odbc>>::decode(value)?;
                Ok(Self::parse_from_str(text, $format)?)
            }
        }
    };
}
temporal!(NaiveDate, "DATE", "%Y-%m-%d");
temporal!(NaiveTime, "TIME", "%H:%M:%S%.f");
temporal!(NaiveDateTime, "TIMESTAMP", "%Y-%m-%d %H:%M:%S%.f");

fn date(value: NaiveDate) -> Result<odbc_api::sys::Date, BoxDynError> {
    if !(1..=9999).contains(&value.year()) {
        return Err("ODBC date year must be between 1 and 9999".into());
    }
    Ok(odbc_api::sys::Date {
        year: value.year().try_into()?,
        month: value.month().try_into()?,
        day: value.day().try_into()?,
    })
}
pub(crate) fn native(value: &OdbcValue) -> Result<Box<dyn InputParameter>, BoxDynError> {
    Ok(match value.type_info.0.as_ref() {
        "DATE" => Box::new(match value.data {
            None => Nullable::<odbc_api::sys::Date>::null(),
            Some(_) => Nullable::new(date(NaiveDate::decode(OdbcValueRef(value))?)?),
        }),
        "TIME" => {
            let parameter = match value.data {
                None => Nullable::<odbc_api::sys::Time>::null(),
                Some(_) => {
                    let time = NaiveTime::decode(OdbcValueRef(value))?;
                    if time.nanosecond() != 0 {
                        return Err("ODBC TIME cannot preserve fractional seconds".into());
                    }
                    Nullable::new(odbc_api::sys::Time {
                        hour: time.hour().try_into()?,
                        minute: time.minute().try_into()?,
                        second: time.second().try_into()?,
                    })
                }
            };
            Box::new(WithDataType {
                value: parameter,
                data_type: DataType::Time { precision: 0 },
            })
        }
        "TIMESTAMP" => {
            let parameter = match value.data {
                None => Nullable::<odbc_api::sys::Timestamp>::null(),
                Some(_) => {
                    let timestamp = NaiveDateTime::decode(OdbcValueRef(value))?;
                    let date = date(timestamp.date())?;
                    if timestamp.nanosecond() >= 1_000_000_000 {
                        return Err("ODBC timestamp cannot represent leap seconds".into());
                    }
                    Nullable::new(odbc_api::sys::Timestamp {
                        year: date.year,
                        month: date.month,
                        day: date.day,
                        hour: timestamp.hour().try_into()?,
                        minute: timestamp.minute().try_into()?,
                        second: timestamp.second().try_into()?,
                        fraction: timestamp.nanosecond(),
                    })
                }
            };
            Box::new(WithDataType {
                value: parameter,
                data_type: DataType::Timestamp { precision: 9 },
            })
        }
        _ => return Err("unsupported ODBC temporal type".into()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OdbcArguments;
    use sqlx_core::{arguments::Arguments, value::Value};
    #[test]
    fn temporal_values_and_nulls_preserve_types() {
        let date = NaiveDate::from_ymd_opt(2024, 2, 29).unwrap();
        let timestamp = date.and_hms_nano_opt(23, 59, 59, 123456789).unwrap();
        let time = NaiveTime::from_hms_opt(23, 59, 59).unwrap();
        let mut arguments = OdbcArguments::default();
        arguments.add(date).unwrap();
        arguments.add(time).unwrap();
        arguments.add(timestamp).unwrap();
        arguments.add(None::<NaiveDateTime>).unwrap();
        assert_eq!(arguments.0[0].try_decode::<NaiveDate>().unwrap(), date);
        assert_eq!(arguments.0[1].try_decode::<NaiveTime>().unwrap(), time);
        assert_eq!(
            arguments.0[2].try_decode::<NaiveDateTime>().unwrap(),
            timestamp
        );
        let parameters = arguments.to_native_parameters().unwrap();
        assert_eq!(
            parameters[3].data_type(),
            DataType::Timestamp { precision: 9 }
        );
    }
    #[test]
    fn unrepresentable_values_fail_without_mutating_arguments() {
        let mut arguments = OdbcArguments::default();
        assert!(arguments
            .add(NaiveDate::from_ymd_opt(0, 1, 1).unwrap())
            .is_err());
        assert!(arguments
            .add(NaiveTime::from_hms_nano_opt(1, 2, 3, 1).unwrap())
            .is_err());
        assert_eq!(arguments.len(), 0);
    }
}
