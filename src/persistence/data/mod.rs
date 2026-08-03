pub mod rkyv_data;
mod types;
mod util;

pub use types::DataTypeValue;

use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataDecodeError {
    UnsupportedDataType {
        data_type: String,
    },
    BufferTooShort {
        required: usize,
        actual: usize,
    },
    FieldOutOfBounds {
        field_index: usize,
        field_end: usize,
        actual: usize,
    },
    InvalidArchive {
        data_type: &'static str,
        message: String,
    },
}

impl fmt::Display for DataDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedDataType { data_type } => {
                write!(f, "unsupported archived row data type `{data_type}`")
            }
            Self::BufferTooShort { required, actual } => write!(
                f,
                "archived row buffer is too short: schema requires {required} bytes, buffer has {actual}"
            ),
            Self::FieldOutOfBounds {
                field_index,
                field_end,
                actual,
            } => write!(
                f,
                "archived row field {field_index} ends at byte {field_end}, beyond buffer length {actual}"
            ),
            Self::InvalidArchive { data_type, message } => {
                write!(f, "invalid archived `{data_type}` field: {message}")
            }
        }
    }
}

impl std::error::Error for DataDecodeError {}

pub trait DataType {
    /// Advances an offset past this type, including its required padding.
    fn advance_accum(&self, accum: &mut usize);

    /// Validates and decodes a value rooted at the end of `bytes`.
    #[allow(clippy::wrong_self_convention)]
    fn from_archived_bytes(&self, bytes: &[u8]) -> Result<DataTypeValue, DataDecodeError>;
}
