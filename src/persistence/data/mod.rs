pub mod rkyv_data;
mod types;
mod util;

pub use types::DataTypeValue;

pub trait DataType {
    fn advance_accum(&self, accum: &mut usize);
    #[allow(clippy::wrong_self_convention)]
    fn from_pointer(&self, pointer: *const u8, start_pointer: *const u8) -> DataTypeValue;
    fn advance_pointer_for_padding(&self, pointer: &mut *const u8, start_pointer: *const u8);
    fn advance_pointer(&self, pointer: &mut *const u8);
}
