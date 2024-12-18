use std::str::FromStr;

use derive_more::From;
use rkyv::primitive::{
    ArchivedF32, ArchivedF64, ArchivedI128, ArchivedI16, ArchivedI32, ArchivedI64, ArchivedU128,
    ArchivedU16, ArchivedU32, ArchivedU64,
};
use rkyv::string::ArchivedString;

use crate::persistence::data::util::{advance_accum_for_padding, advance_pointer_for_padding};
use crate::persistence::data::DataType;

#[derive(Debug, From, PartialEq)]
pub enum DataTypeValue {
    String(String),
    I128(i128),
    I64(i64),
    I32(i32),
    I16(i16),
    I8(i8),
    U128(u128),
    U64(u64),
    U32(u32),
    U16(u16),
    U8(u8),
    F64(f64),
    F32(f32),
}

impl DataTypeValue {
    pub fn as_data_type(&self) -> &dyn DataType {
        match self {
            Self::String(s) => s,
            Self::I128(i) => i,
            Self::I64(i) => i,
            Self::I32(i) => i,
            Self::I16(i) => i,
            Self::I8(i) => i,
            Self::U128(u) => u,
            Self::U64(u) => u,
            Self::U32(u) => u,
            Self::U16(u) => u,
            Self::U8(u) => u,
            Self::F64(f) => f,
            Self::F32(f) => f,
        }
    }
}

impl FromStr for DataTypeValue {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.as_ref() {
            "String" => String::default().into(),
            "i32" => i32::default().into(),
            _ => unreachable!(),
        })
    }
}

impl DataType for String {
    fn advance_accum(&self, accum: &mut usize) {
        *accum = advance_accum_for_padding(*accum, 4);
        *accum += size_of::<ArchivedString>();
    }

    fn from_pointer(&self, pointer: *const u8, start_pointer: *const u8) -> DataTypeValue {
        let current_pointer = advance_pointer_for_padding(pointer, start_pointer, 4);
        let archived_ptr: *const ArchivedString = current_pointer.cast();
        unsafe { (*archived_ptr).to_string() }.into()
    }

    fn advance_pointer(&self, pointer: &mut *const u8) {
        *pointer = unsafe { pointer.add(size_of::<ArchivedString>()) };
    }
}

macro_rules! impl_datatype {
    ($datatype:ty, $archived_datatype:ty) => {
        impl DataType for $datatype {
            fn advance_accum(&self, accum: &mut usize) {
                *accum = advance_accum_for_padding(*accum, size_of::<$archived_datatype>());
                *accum += size_of::<$archived_datatype>();
            }

            fn from_pointer(&self, pointer: *const u8, start_pointer: *const u8) -> DataTypeValue {
                let current_pointer = advance_pointer_for_padding(
                    pointer,
                    start_pointer,
                    size_of::<$archived_datatype>(),
                );
                let archived_ptr: *const $archived_datatype = current_pointer.cast();
                unsafe { (*archived_ptr).to_string() }.into()
            }

            fn advance_pointer(&self, pointer: &mut *const u8) {
                *pointer = unsafe { pointer.add(size_of::<$archived_datatype>()) };
            }
        }
    };
}

impl_datatype! {i128, ArchivedI128}
impl_datatype! {i64, ArchivedI64}
impl_datatype! {i32, ArchivedI32}
impl_datatype! {i16, ArchivedI16}
impl_datatype! {i8, i8}
impl_datatype! {u128, ArchivedU128}
impl_datatype! {u64, ArchivedU64}
impl_datatype! {u32, ArchivedU32}
impl_datatype! {u16, ArchivedU16}
impl_datatype! {u8, u8}
impl_datatype! {f64, ArchivedF64}
impl_datatype! {f32, ArchivedF32}
