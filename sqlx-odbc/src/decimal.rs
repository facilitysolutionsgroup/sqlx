use crate::{Odbc, OdbcTypeInfo, OdbcValue, OdbcValueRef};
use odbc_api::{
    parameter::{InputParameter, VarCharBox, WithDataType},
    DataType,
};
use rust_decimal::Decimal;
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};

impl Type<Odbc> for Decimal {
    fn type_info() -> OdbcTypeInfo {
        OdbcTypeInfo("DECIMAL".into())
    }
}
impl Encode<'_, Odbc> for Decimal {
    fn encode_by_ref(&self, buffer: &mut Vec<OdbcValue>) -> Result<IsNull, BoxDynError> {
        buffer.push(OdbcValue {
            type_info: <Self as Type<Odbc>>::type_info(),
            data: Some(self.to_string().as_bytes().into()),
        });
        Ok(IsNull::No)
    }
}
impl Decode<'_, Odbc> for Decimal {
    fn decode(value: OdbcValueRef<'_>) -> Result<Self, BoxDynError> {
        Ok(Decimal::from_str_exact(<&str as Decode<Odbc>>::decode(
            value,
        )?)?)
    }
}
pub(crate) fn native(value: &OdbcValue) -> Result<Box<dyn InputParameter>, BoxDynError> {
    let (parameter, precision, scale) = match &value.data {
        None => (VarCharBox::null(), 29, 0),
        Some(_) => {
            let decimal = <Decimal as Decode<Odbc>>::decode(OdbcValueRef(value))?;
            let scale = i16::try_from(decimal.scale())?;
            let digits = decimal.mantissa().unsigned_abs().to_string().len();
            let precision = std::cmp::max(digits, usize::try_from(scale)?);
            (
                VarCharBox::from_string(decimal.to_string()),
                precision,
                scale,
            )
        }
    };
    Ok(Box::new(WithDataType {
        value: parameter,
        data_type: DataType::Decimal { precision, scale },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OdbcArguments;
    use sqlx_core::{arguments::Arguments, value::Value};
    #[test]
    fn decimals_preserve_scale_and_extremes() {
        for text in [
            "0",
            "-0.0001",
            "123.4500",
            "79228162514264337593543950335",
            "0.0000000000000000000000000001",
        ] {
            let decimal = Decimal::from_str_exact(text).unwrap();
            let mut arguments = OdbcArguments::default();
            arguments.add(decimal).unwrap();
            let decoded = arguments.0[0].try_decode::<Decimal>().unwrap();
            assert_eq!(decoded, decimal);
            assert_eq!(decoded.scale(), decimal.scale());
            let native = arguments.to_native_parameters().unwrap();
            let DataType::Decimal { scale, .. } = native[0].data_type() else {
                panic!("expected decimal")
            };
            assert_eq!(u32::try_from(scale).unwrap(), decimal.scale());
        }
        let mut arguments = OdbcArguments::default();
        arguments.add(None::<Decimal>).unwrap();
        assert!(matches!(
            arguments.to_native_parameters().unwrap()[0].data_type(),
            DataType::Decimal { .. }
        ));
    }
}
