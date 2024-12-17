pub mod rkyv_data;
mod types;
mod util;

pub use types::DataTypeValue;

pub trait DataType {
    fn advance_accum(&self, accum: &mut usize);
    fn from_pointer(&self, pointer: *const u8, start_pointer: *const u8) -> DataTypeValue;
    fn advance_pointer(&self, pointer: *const u8);
}
