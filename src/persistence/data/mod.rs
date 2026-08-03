pub mod rkyv_data;
mod types;
mod util;

pub use types::DataTypeValue;

pub trait DataType {
    fn advance_accum(&self, accum: &mut usize);

    #[allow(clippy::wrong_self_convention)]
    fn from_archived_bytes(&self, _bytes: &[u8]) -> DataTypeValue {
        panic!("this data type does not implement validated archive decoding")
    }

    #[deprecated(note = "raw pointers cannot be validated by a safe API; use from_archived_bytes")]
    #[allow(clippy::wrong_self_convention)]
    fn from_pointer(&self, _pointer: *const u8, _start_pointer: *const u8) -> DataTypeValue {
        panic!("raw-pointer row decoding is no longer supported by this safe API")
    }

    #[deprecated(note = "raw-pointer row decoding is no longer supported")]
    fn advance_pointer_for_padding(&self, _pointer: &mut *const u8, _start_pointer: *const u8) {
        panic!("raw-pointer row decoding is no longer supported by this safe API")
    }

    #[deprecated(note = "raw-pointer row decoding is no longer supported")]
    fn advance_pointer(&self, _pointer: &mut *const u8) {
        panic!("raw-pointer row decoding is no longer supported by this safe API")
    }
}
