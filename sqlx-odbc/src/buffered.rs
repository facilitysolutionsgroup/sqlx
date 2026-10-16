use crate::{error::map_error, execution::Output, OdbcColumn, OdbcRow, OdbcValue};
use odbc_api::{
    buffers::{BufferDesc, ColumnarDynBuffer},
    Cursor,
};
use sqlx_core::{error::Error, Either};
use std::sync::Arc;

pub(crate) fn fetch<C: Cursor>(
    cursor: C,
    columns: Arc<[OdbcColumn]>,
    output: &flume::Sender<Output>,
) -> Result<C, Error> {
    fetch_with_capacity(cursor, columns, output, 32768)
}

fn fetch_with_capacity<C: Cursor>(
    cursor: C,
    columns: Arc<[OdbcColumn]>,
    output: &flume::Sender<Output>,
    text_capacity: usize,
) -> Result<C, Error> {
    let descriptions = columns
        .iter()
        .map(|column| match column.type_info.0.as_ref() {
            "TINYINT" => BufferDesc::I8 { nullable: true },
            "SMALLINT" => BufferDesc::I16 { nullable: true },
            "INTEGER" => BufferDesc::I32 { nullable: true },
            "BIGINT" => BufferDesc::I64 { nullable: true },
            "REAL" => BufferDesc::F32 { nullable: true },
            "DOUBLE" => BufferDesc::F64 { nullable: true },
            "BIT" => BufferDesc::Bit { nullable: true },
            "VARBINARY" => BufferDesc::Binary { max_bytes: 65536 },
            _ => BufferDesc::WText {
                max_str_len: text_capacity,
            },
        });
    let buffer = ColumnarDynBuffer::try_from_descs(32, descriptions).map_err(map_error)?;
    let mut block = cursor.bind_buffer(buffer).map_err(map_error)?;
    'batches: while let Some(batch) = block.fetch_with_truncation_check(true).map_err(map_error)? {
        for index in 0..batch.num_rows() {
            let mut values = Vec::with_capacity(columns.len());
            for (ordinal, column) in columns.iter().enumerate() {
                let slice = batch.column(ordinal);
                macro_rules! scalar {
                    ($rust:ty) => {
                        slice
                            .as_nullable_slice::<$rust>()
                            .unwrap()
                            .get(index)
                            .map(|value| Arc::<[u8]>::from(value.to_le_bytes().as_slice()))
                    };
                }
                let data = match column.type_info.0.as_ref() {
                    "TINYINT" => scalar!(i8),
                    "SMALLINT" => scalar!(i16),
                    "INTEGER" => scalar!(i32),
                    "BIGINT" => scalar!(i64),
                    "REAL" => scalar!(f32),
                    "DOUBLE" => scalar!(f64),
                    "BIT" => slice
                        .as_nullable_slice::<odbc_api::Bit>()
                        .unwrap()
                        .get(index)
                        .map(|value| Arc::<[u8]>::from([value.0])),
                    "VARBINARY" => slice.as_binary().unwrap().get(index).map(Arc::from),
                    _ => slice
                        .as_wide_text()
                        .unwrap()
                        .get(index)
                        .map(|text| {
                            String::from_utf16(text)
                                .map(|text| Arc::<[u8]>::from(text.into_bytes()))
                        })
                        .transpose()
                        .map_err(|error| Error::Decode(Box::new(error)))?,
                };
                values.push(OdbcValue {
                    type_info: column.type_info.clone(),
                    data,
                });
            }
            let row = OdbcRow {
                columns: columns.clone(),
                values: values.into(),
            };
            if output.send(Ok(Either::Right(row))).is_err() {
                break 'batches;
            }
        }
    }
    block.unbind().map(|(cursor, _)| cursor).map_err(map_error)
}
