use crate::{Odbc, OdbcArguments, OdbcTypeInfo, OdbcValue, OdbcValueRef};
use odbc_api::{
    parameter::{InputParameter, VarBinaryBox, VarWCharBox},
    Nullable,
};
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::{BoxDynError, UnexpectedNullError},
    types::Type,
};

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx_core::{arguments::Arguments, value::Value};

    fn round_trip<T>(value: T)
    where
        T: for<'r> Encode<'r, Odbc>
            + for<'r> Decode<'r, Odbc>
            + Type<Odbc>
            + PartialEq
            + std::fmt::Debug
            + Clone,
    {
        let mut arguments = OdbcArguments::default();
        arguments.add(value.clone()).unwrap();
        assert_eq!(arguments.0[0].try_decode::<T>().unwrap(), value);
        let native = arguments.to_native_parameters().unwrap();
        assert_eq!(native.len(), 1);
        native[0].assert_completness();
    }
    #[test]
    fn scalar_boundaries_round_trip() {
        round_trip(i8::MIN);
        round_trip(i8::MAX);
        round_trip(i16::MIN);
        round_trip(i16::MAX);
        round_trip(i32::MIN);
        round_trip(i32::MAX);
        round_trip(i64::MIN);
        round_trip(i64::MAX);
        round_trip(f32::MIN);
        round_trip(f32::MAX);
        round_trip(f64::MIN);
        round_trip(f64::MAX);
        round_trip(true);
        round_trip(false);
    }
    #[test]
    fn decimal_to_float_compatibility() {
        for (text, expected, expected_f32) in [
            ("0.00", 0.0, 0.0_f32),
            (" -123.50 ", -123.5, -123.5_f32),
            ("123456789.25", 123456789.25, 123456792.0_f32),
        ] {
            let value = OdbcValue {
                type_info: OdbcTypeInfo("DECIMAL".into()),
                data: Some(text.as_bytes().into()),
            };
            assert_eq!(value.try_decode::<f64>().unwrap(), expected);
            assert_eq!(value.try_decode::<f32>().unwrap(), expected_f32);
            assert_eq!(value.try_decode::<Option<f64>>().unwrap(), Some(expected));
            assert_eq!(
                <f64 as Decode<Odbc>>::decode(OdbcValueRef(&value)).unwrap(),
                expected
            );
        }
        let null = OdbcValue {
            type_info: OdbcTypeInfo("DECIMAL".into()),
            data: None,
        };
        assert_eq!(null.try_decode::<Option<f64>>().unwrap(), None);
        assert_eq!(null.try_decode::<Option<f32>>().unwrap(), None);
        assert!(null.try_decode::<f64>().is_err());
        for data in [b"".as_slice(), b"invalid", b"1e999", b"NaN", b"inf", &[255]] {
            let value = OdbcValue {
                type_info: OdbcTypeInfo("DECIMAL".into()),
                data: Some(data.into()),
            };
            assert!(value.try_decode::<f64>().is_err());
            assert!(value.try_decode::<f32>().is_err());
        }
        let value = OdbcValue {
            type_info: OdbcTypeInfo("DECIMAL".into()),
            data: Some(b"1e40".as_slice().into()),
        };
        assert_eq!(value.try_decode::<f64>().unwrap(), 1e40);
        assert!(value.try_decode::<f32>().is_err());
        let text = OdbcValue {
            type_info: OdbcTypeInfo("VARCHAR".into()),
            data: Some(b"123.5".as_slice().into()),
        };
        assert!(text.try_decode::<f64>().is_err());
        assert!(<f64 as Decode<Odbc>>::decode(OdbcValueRef(&text)).is_err());
    }
    #[test]
    fn empty_unicode_and_large_values() {
        round_trip(String::new());
        round_trip("Unicode: Ελληνικά 日本語\0end".to_owned());
        round_trip("x".repeat(1024 * 1024));
        let mut empty = OdbcArguments::default();
        empty.add(Vec::<u8>::new()).unwrap();
        assert!(empty.to_native_parameters().is_err());
        round_trip(vec![0, 255, 0, 128]);
    }
    #[test]
    fn text_parameters_use_wide_character_binding() {
        for value in [Some(""), Some("Ελληνικά 日本語"), Some("😀"), None] {
            let mut arguments = OdbcArguments::default();
            arguments.add(value).unwrap();
            let native = arguments.to_native_parameters().unwrap();
            assert_eq!(native[0].cdata_type(), odbc_api::sys::CDataType::WChar);
            assert!(matches!(
                native[0].data_type(),
                odbc_api::DataType::WVarchar { .. }
            ));
            native[0].assert_completness();
        }
    }
    #[test]
    fn nulls_keep_native_sql_types() {
        let mut arguments = OdbcArguments::default();
        arguments.add(None::<i64>).unwrap();
        arguments.add(None::<String>).unwrap();
        arguments.add(None::<Vec<u8>>).unwrap();
        arguments.add(None::<bool>).unwrap();
        let native = arguments.to_native_parameters().unwrap();
        assert_eq!(native[0].data_type(), odbc_api::DataType::BigInt);
        for parameter in native {
            parameter.assert_completness();
        }
        assert_eq!(arguments.0[0].try_decode::<Option<i64>>().unwrap(), None);
        assert!(arguments.0[0].try_decode::<i64>().is_err());
    }
    #[test]
    fn malformed_values_are_not_silently_converted() {
        let value = OdbcValue {
            type_info: OdbcTypeInfo("VARCHAR".into()),
            data: Some(vec![255].into()),
        };
        assert!(value.try_decode::<String>().is_err());
        assert!(OdbcArguments(vec![value]).to_native_parameters().is_err());
        let value = OdbcValue {
            type_info: OdbcTypeInfo("INTEGER".into()),
            data: Some(vec![0].into()),
        };
        assert!(value.try_decode::<i32>().is_err());
        assert!(value.try_decode::<i64>().is_err());
        let value = OdbcValue {
            type_info: OdbcTypeInfo("BIT".into()),
            data: Some(vec![2].into()),
        };
        assert!(value.try_decode::<bool>().is_err());
    }
}

fn bytes(value: OdbcValueRef<'_>) -> Result<&[u8], BoxDynError> {
    value
        .0
        .data
        .as_deref()
        .ok_or_else(|| Box::new(UnexpectedNullError) as BoxDynError)
}
fn put(buffer: &mut Vec<OdbcValue>, name: &str, data: &[u8]) -> IsNull {
    buffer.push(OdbcValue {
        type_info: OdbcTypeInfo(name.into()),
        data: Some(data.into()),
    });
    IsNull::No
}
macro_rules! scalar {
    ($rust:ty, $name:literal) => {
        impl Type<Odbc> for $rust {
            fn type_info() -> OdbcTypeInfo {
                OdbcTypeInfo($name.into())
            }
        }
        impl Encode<'_, Odbc> for $rust {
            fn encode_by_ref(&self, buffer: &mut Vec<OdbcValue>) -> Result<IsNull, BoxDynError> {
                Ok(put(buffer, $name, &self.to_le_bytes()))
            }
        }
        impl Decode<'_, Odbc> for $rust {
            fn decode(value: OdbcValueRef<'_>) -> Result<Self, BoxDynError> {
                if value.0.type_info != <Self as Type<Odbc>>::type_info() {
                    return Err("incompatible ODBC scalar representation".into());
                }
                Ok(Self::from_le_bytes(bytes(value)?.try_into()?))
            }
        }
    };
}
scalar!(i8, "TINYINT");
scalar!(i16, "SMALLINT");
scalar!(i32, "INTEGER");
scalar!(i64, "BIGINT");
macro_rules! float {
    ($rust:ty, $name:literal) => {
        impl Type<Odbc> for $rust {
            fn type_info() -> OdbcTypeInfo {
                OdbcTypeInfo($name.into())
            }

            fn compatible(ty: &OdbcTypeInfo) -> bool {
                matches!(ty.0.as_ref(), $name | "DECIMAL")
            }
        }
        impl Encode<'_, Odbc> for $rust {
            fn encode_by_ref(&self, buffer: &mut Vec<OdbcValue>) -> Result<IsNull, BoxDynError> {
                Ok(put(buffer, $name, &self.to_le_bytes()))
            }
        }
        impl Decode<'_, Odbc> for $rust {
            fn decode(value: OdbcValueRef<'_>) -> Result<Self, BoxDynError> {
                match value.0.type_info.0.as_ref() {
                    $name => Ok(Self::from_le_bytes(bytes(value)?.try_into()?)),
                    "DECIMAL" => {
                        let number = std::str::from_utf8(bytes(value)?)?.trim().parse::<Self>()?;
                        if !number.is_finite() {
                            return Err("ODBC decimal is out of range for a finite float".into());
                        }
                        Ok(number)
                    }
                    _ => Err("incompatible ODBC scalar representation".into()),
                }
            }
        }
    };
}
float!(f32, "REAL");
float!(f64, "DOUBLE");
impl Type<Odbc> for bool {
    fn type_info() -> OdbcTypeInfo {
        OdbcTypeInfo("BIT".into())
    }
}
impl Encode<'_, Odbc> for bool {
    fn encode_by_ref(&self, buffer: &mut Vec<OdbcValue>) -> Result<IsNull, BoxDynError> {
        Ok(put(buffer, "BIT", &[u8::from(*self)]))
    }
}
impl Decode<'_, Odbc> for bool {
    fn decode(value: OdbcValueRef<'_>) -> Result<Self, BoxDynError> {
        match bytes(value)? {
            [0] => Ok(false),
            [1] => Ok(true),
            _ => Err("invalid ODBC boolean".into()),
        }
    }
}
impl Type<Odbc> for str {
    fn type_info() -> OdbcTypeInfo {
        OdbcTypeInfo("VARCHAR".into())
    }
}
impl Type<Odbc> for String {
    fn type_info() -> OdbcTypeInfo {
        <str as Type<Odbc>>::type_info()
    }
}
impl Encode<'_, Odbc> for str {
    fn encode_by_ref(&self, buffer: &mut Vec<OdbcValue>) -> Result<IsNull, BoxDynError> {
        Ok(put(buffer, "VARCHAR", self.as_bytes()))
    }
}
impl Encode<'_, Odbc> for &str {
    fn encode_by_ref(&self, buffer: &mut Vec<OdbcValue>) -> Result<IsNull, BoxDynError> {
        <str as Encode<Odbc>>::encode_by_ref(self, buffer)
    }
}
impl Encode<'_, Odbc> for String {
    fn encode_by_ref(&self, buffer: &mut Vec<OdbcValue>) -> Result<IsNull, BoxDynError> {
        self.as_str().encode_by_ref(buffer)
    }
}
impl<'r> Decode<'r, Odbc> for &'r str {
    fn decode(value: OdbcValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(std::str::from_utf8(bytes(value)?)?)
    }
}
impl Decode<'_, Odbc> for String {
    fn decode(value: OdbcValueRef<'_>) -> Result<Self, BoxDynError> {
        Ok(<&str as Decode<Odbc>>::decode(value)?.to_owned())
    }
}
impl Type<Odbc> for [u8] {
    fn type_info() -> OdbcTypeInfo {
        OdbcTypeInfo("VARBINARY".into())
    }
}
impl Type<Odbc> for Vec<u8> {
    fn type_info() -> OdbcTypeInfo {
        <[u8] as Type<Odbc>>::type_info()
    }
}
impl Encode<'_, Odbc> for [u8] {
    fn encode_by_ref(&self, buffer: &mut Vec<OdbcValue>) -> Result<IsNull, BoxDynError> {
        Ok(put(buffer, "VARBINARY", self))
    }
}
impl Encode<'_, Odbc> for &[u8] {
    fn encode_by_ref(&self, buffer: &mut Vec<OdbcValue>) -> Result<IsNull, BoxDynError> {
        <[u8] as Encode<Odbc>>::encode_by_ref(self, buffer)
    }
}
impl Encode<'_, Odbc> for Vec<u8> {
    fn encode_by_ref(&self, buffer: &mut Vec<OdbcValue>) -> Result<IsNull, BoxDynError> {
        self.as_slice().encode_by_ref(buffer)
    }
}
impl<'r> Decode<'r, Odbc> for &'r [u8] {
    fn decode(value: OdbcValueRef<'r>) -> Result<Self, BoxDynError> {
        bytes(value)
    }
}
impl Decode<'_, Odbc> for Vec<u8> {
    fn decode(value: OdbcValueRef<'_>) -> Result<Self, BoxDynError> {
        Ok(bytes(value)?.to_vec())
    }
}

impl OdbcArguments {
    pub fn to_native_parameters(&self) -> Result<Vec<Box<dyn InputParameter>>, BoxDynError> {
        self.0
            .iter()
            .map(|value| {
                macro_rules! native {
                    ($rust:ty) => {{
                        let parameter = match value.data {
                            None => Nullable::<$rust>::null(),
                            Some(_) => {
                                Nullable::new(<$rust as Decode<Odbc>>::decode(OdbcValueRef(value))?)
                            }
                        };
                        Box::new(parameter) as Box<dyn InputParameter>
                    }};
                }
                let parameter: Box<dyn InputParameter> = match value.type_info.0.as_ref() {
                    #[cfg(feature = "chrono")]
                    "DATE" | "TIME" | "TIMESTAMP" => crate::temporal::native(value)?,
                    #[cfg(feature = "rust_decimal")]
                    "DECIMAL" => crate::decimal::native(value)?,
                    "TINYINT" => native!(i8),
                    "SMALLINT" => native!(i16),
                    "INTEGER" => native!(i32),
                    "BIGINT" => native!(i64),
                    "REAL" => native!(f32),
                    "DOUBLE" => native!(f64),
                    "BIT" => Box::new(match value.data {
                        None => Nullable::<odbc_api::Bit>::null(),
                        Some(_) => {
                            Nullable::new(odbc_api::Bit(u8::from(<bool as Decode<Odbc>>::decode(
                                OdbcValueRef(value),
                            )?)))
                        }
                    }),
                    "VARCHAR" => Box::new(match &value.data {
                        None => VarWCharBox::null(),
                        Some(data) => {
                            let text = std::str::from_utf8(data)?;
                            if text.is_empty() {
                                VarWCharBox::from_buffer(
                                    vec![0u16].into_boxed_slice(),
                                    odbc_api::buffers::Indicator::Length(0),
                                )
                            } else {
                                VarWCharBox::from_str_slice(text)
                            }
                        }
                    }),
                    "VARBINARY" if value.data.as_ref().is_some_and(|data| data.is_empty()) => return Err("empty binary parameters are unsupported: some ODBC drivers bind them as a zero byte".into()),
                    "VARBINARY" => Box::new(match &value.data {
                        None => VarBinaryBox::null(),
                        Some(data) => VarBinaryBox::from_buffer(
                            data.to_vec().into_boxed_slice(),
                            odbc_api::buffers::Indicator::Length(data.len()),
                        ),
                    }),
                    _ => return Err("unsupported ODBC parameter type".into()),
                };
                Ok(parameter)
            })
            .collect()
    }
}
