use std::str::FromStr;

use derive_more::From;
use rkyv::string::ArchivedString;

use crate::persistence::data::util::{advance_accum_for_padding, advance_pointer_for_padding};
use crate::persistence::data::DataType;

#[derive(Debug, From, PartialEq)]
pub enum DataTypeValue {
    String(String),
    // TODO: add other types.
}

impl DataTypeValue {
    pub fn as_data_type(&self) -> &dyn DataType {
        match self {
            Self::String(s) => s,
            _ => unreachable!(),
        }
    }
}

impl FromStr for DataTypeValue {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.as_ref() {
            "String" => String::default().into(),
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

    fn advance_pointer(&self, pointer: *const u8) {
        unsafe { pointer.add(size_of::<ArchivedString>()) };
    }
}
