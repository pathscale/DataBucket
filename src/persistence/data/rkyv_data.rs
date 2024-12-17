use crate::persistence::data::types::DataTypeValue;
use crate::persistence::data::util::{advance_accum_for_padding, advance_pointer_for_padding};
use rkyv::{
    primitive::{
        ArchivedF32, ArchivedF64, ArchivedI128, ArchivedI16, ArchivedI32, ArchivedI64,
        ArchivedU128, ArchivedU16, ArchivedU32, ArchivedU64,
    },
    string::ArchivedString,
};
use std::str::FromStr;

pub fn parse_archived_row<S1: AsRef<str>, S2: AsRef<str>>(
    buf: &[u8],
    columns: &Vec<(S1, S2)>,
) -> Vec<DataTypeValue> {
    let mut data_length: usize = {
        let mut accum: usize = 0;
        for column in columns.iter() {
            let value =
                DataTypeValue::from_str(column.1.as_ref()).expect("data type should be supported");
            let data_type = value.as_data_type();
            data_type.advance_accum(&mut accum);

            // match column.1.as_str() {
            //     "String" => {
            //         accum = advance_accum_for_padding(accum, 4);
            //         accum += size_of::<ArchivedString>();
            //     }
            //
            //     "i128" => {
            //         accum = advance_accum_for_padding(accum, size_of::<ArchivedI128>());
            //         accum += size_of::<ArchivedI128>();
            //     }
            //     "i64" => {
            //         accum = advance_accum_for_padding(accum, size_of::<ArchivedI64>());
            //         accum += size_of::<ArchivedI64>();
            //     }
            //     "i32" => {
            //         accum = advance_accum_for_padding(accum, size_of::<ArchivedI32>());
            //         accum += size_of::<ArchivedI32>();
            //     }
            //     "i16" => {
            //         accum = advance_accum_for_padding(accum, size_of::<ArchivedI16>());
            //         accum += size_of::<ArchivedI16>();
            //     }
            //     "i8" => accum += size_of::<i8>(),
            //
            //     "u128" => {
            //         accum = advance_accum_for_padding(accum, size_of::<ArchivedU128>());
            //         accum += size_of::<ArchivedU128>();
            //     }
            //     "u64" => {
            //         accum = advance_accum_for_padding(accum, size_of::<ArchivedU64>());
            //         accum += size_of::<ArchivedU64>();
            //     }
            //     "u32" => {
            //         accum = advance_accum_for_padding(accum, size_of::<ArchivedU32>());
            //         accum += size_of::<ArchivedU32>();
            //     }
            //     "u16" => {
            //         accum = advance_accum_for_padding(accum, size_of::<ArchivedU16>());
            //         accum += size_of::<ArchivedU16>();
            //     }
            //     "u8" => accum += size_of::<u8>(),
            //
            //     "f64" => {
            //         accum = advance_accum_for_padding(accum, size_of::<ArchivedF64>());
            //         accum += size_of::<ArchivedF64>();
            //     }
            //     "f32" => {
            //         accum = advance_accum_for_padding(accum, size_of::<ArchivedF32>());
            //         accum += size_of::<ArchivedF32>();
            //     }
            //
            //     _ => panic!("Unknown data type {:?}", column.1),
            // }
        }
        accum
    };
    if data_length % 4 != 0 {
        data_length += 4 - data_length % 4;
    }

    let start_pointer = unsafe { buf.as_ptr().add(buf.len()).sub(data_length) };
    let current_pointer = start_pointer;
    let mut output: Vec<_> = vec![];
    for column in columns.iter() {
        let value =
            DataTypeValue::from_str(column.1.as_ref()).expect("data type should be supported");
        let data_type = value.as_data_type();
        let deserialized = data_type.from_pointer(current_pointer, start_pointer);
        data_type.advance_pointer(current_pointer);
        output.push(deserialized);

        // match column.1.as_str() {
        //     "String" => {
        //         current_pointer = advance_pointer_for_padding(current_pointer, start_pointer, 4);
        //         let archived_ptr: *const ArchivedString = current_pointer.cast();
        //         output.push(unsafe { (*archived_ptr).to_string() });
        //         current_pointer =
        //             unsafe { current_pointer.add(size_of::<ArchivedString>()) };
        //     }
        //
        //     "i128" => {
        //         current_pointer = advance_pointer_for_padding(
        //             current_pointer,
        //             start_pointer,
        //             size_of::<ArchivedI128>(),
        //         );
        //         let archived_ptr: *const ArchivedI128 = current_pointer.cast();
        //         output.push(unsafe { (*archived_ptr).to_string() });
        //         current_pointer =
        //             unsafe { current_pointer.add(size_of::<ArchivedI128>()) };
        //     }
        //     "i64" => {
        //         current_pointer = advance_pointer_for_padding(
        //             current_pointer,
        //             start_pointer,
        //             size_of::<ArchivedI64>(),
        //         );
        //         let archived_ptr: *const ArchivedI64 = current_pointer.cast();
        //         output.push(unsafe { (*archived_ptr).to_string() });
        //         current_pointer =
        //             unsafe { current_pointer.add(size_of::<ArchivedI64>()) };
        //     }
        //     "i32" => {
        //         current_pointer = advance_pointer_for_padding(
        //             current_pointer,
        //             start_pointer,
        //             size_of::<ArchivedI32>(),
        //         );
        //         let archived_ptr: *const ArchivedI32 = current_pointer.cast();
        //         output.push(unsafe { (*archived_ptr).to_string() });
        //         current_pointer =
        //             unsafe { current_pointer.add(size_of::<ArchivedI32>()) };
        //     }
        //     "i16" => {
        //         current_pointer = advance_pointer_for_padding(
        //             current_pointer,
        //             start_pointer,
        //             size_of::<ArchivedI16>(),
        //         );
        //         let archived_ptr: *const ArchivedI16 = current_pointer.cast();
        //         output.push(unsafe { (*archived_ptr).to_string() });
        //         current_pointer =
        //             unsafe { current_pointer.add(size_of::<ArchivedI16>()) };
        //     }
        //     "i8" => {
        //         let archived_ptr: *const i8 = current_pointer.cast();
        //         output.push(unsafe { (*archived_ptr).to_string() });
        //         current_pointer = unsafe { current_pointer.add(size_of::<i8>()) };
        //     }
        //
        //     "u128" => {
        //         current_pointer = advance_pointer_for_padding(
        //             current_pointer,
        //             start_pointer,
        //             size_of::<ArchivedU128>(),
        //         );
        //         let archived_ptr: *const ArchivedU128 = current_pointer.cast();
        //         output.push(unsafe { (*archived_ptr).to_string() });
        //         current_pointer =
        //             unsafe { current_pointer.add(size_of::<ArchivedU128>()) };
        //     }
        //     "u64" => {
        //         current_pointer = advance_pointer_for_padding(
        //             current_pointer,
        //             start_pointer,
        //             size_of::<ArchivedU64>(),
        //         );
        //         let archived_ptr: *const ArchivedU64 = current_pointer.cast();
        //         output.push(unsafe { (*archived_ptr).to_string() });
        //         current_pointer =
        //             unsafe { current_pointer.add(size_of::<ArchivedU64>()) };
        //     }
        //     "u32" => {
        //         current_pointer = advance_pointer_for_padding(
        //             current_pointer,
        //             start_pointer,
        //             size_of::<ArchivedU32>(),
        //         );
        //         let archived_ptr: *const ArchivedU32 = current_pointer.cast();
        //         output.push(unsafe { (*archived_ptr).to_string() });
        //         current_pointer =
        //             unsafe { current_pointer.add(size_of::<ArchivedU32>()) };
        //     }
        //     "u16" => {
        //         current_pointer = advance_pointer_for_padding(
        //             current_pointer,
        //             start_pointer,
        //             size_of::<ArchivedU16>(),
        //         );
        //         let archived_ptr: *const ArchivedU16 = current_pointer.cast();
        //         output.push(unsafe { (*archived_ptr).to_string() });
        //         current_pointer =
        //             unsafe { current_pointer.add(size_of::<ArchivedU16>()) };
        //     }
        //     "u8" => {
        //         let archived_ptr: *const u8 = current_pointer.cast();
        //         output.push(unsafe { (*archived_ptr).to_string() });
        //         current_pointer = unsafe { current_pointer.add(size_of::<u8>()) };
        //     }
        //
        //     "f64" => {
        //         current_pointer = advance_pointer_for_padding(
        //             current_pointer,
        //             start_pointer,
        //             size_of::<ArchivedF64>(),
        //         );
        //         let archived_ptr: *const ArchivedF64 = current_pointer.cast();
        //         output.push(unsafe { (*archived_ptr).to_string() });
        //         current_pointer =
        //             unsafe { current_pointer.add(size_of::<ArchivedF64>()) };
        //     }
        //     "f32" => {
        //         current_pointer = advance_pointer_for_padding(
        //             current_pointer,
        //             start_pointer,
        //             size_of::<ArchivedF32>(),
        //         );
        //         let archived_ptr: *const ArchivedF32 = current_pointer.cast();
        //         output.push(unsafe { (*archived_ptr).to_string() });
        //         current_pointer =
        //             unsafe { current_pointer.add(size_of::<ArchivedF32>()) };
        //     }
        //
        //     _ => panic!("Unknown data type: {:?}", column.1),
        // }
    }
    output
}

#[cfg(test)]
mod test {
    use super::parse_archived_row;
    use crate::persistence::data::{self, types::DataTypeValue};
    use rkyv::{Archive, Deserialize, Serialize};

    #[derive(Archive, Serialize, Deserialize, Debug)]
    struct Struct {
        pub string1: String,
        pub int1: u32,
        pub string2: String,
        pub int2: u8,
        pub int3: i8,
        pub int4: u8,
        pub int5: i32,
        pub int6: u8,
        pub string3: String,
        pub int7: i8,
        pub float1: f64,
    }

    #[derive(Archive, Serialize, Deserialize, Debug)]
    struct Struct1 {
        pub string1: String,
    }

    #[test]
    fn test_parse_archived_row() {
        let buffer = rkyv::to_bytes::<rkyv::rancor::Error>(&Struct1 {
            string1: "000000000000000".to_string(),
        })
        .unwrap();
        let parsed = parse_archived_row(&buffer, &vec![("string1", "String")]);
        assert_eq!(
            parsed,
            [DataTypeValue::String("000000000000000".to_string())]
        )
    }

    // TODO: make this test working after other types are added.

    // #[test]
    // fn test_parse_archived_row() {
    //     let buffer = rkyv::to_bytes::<rkyv::rancor::Error>(&Struct {
    //         string1: "000000000000000".to_string(),
    //         int1: 20,
    //         string2: "aaaaaaaa".to_string(),
    //         int2: 3,
    //         int3: 4,
    //         int4: 5,
    //         int5: 6,
    //         int6: 7,
    //         string3: "x".to_owned(),
    //         int7: 8,
    //         float1: 3.14159265358,
    //     })
    //     .unwrap();
    //     let parsed = parse_archived_row(
    //         &buffer,
    //         &vec![
    //             ("string1".to_string(), "String".to_string()),
    //             ("int1".to_string(), "i32".to_string()),
    //             ("string2".to_string(), "String".to_string()),
    //             ("int2".to_string(), "u8".to_string()),
    //             ("int3".to_string(), "i8".to_string()),
    //             ("int4".to_string(), "u8".to_string()),
    //             ("int5".to_string(), "i32".to_string()),
    //             ("int6".to_string(), "u8".to_string()),
    //             ("string3".to_string(), "String".to_string()),
    //             ("int7".to_string(), "i8".to_string()),
    //             ("float1".to_string(), "f64".to_string()),
    //         ],
    //     );
    //     assert_eq!(
    //         parsed,
    //         [
    //             "000000000000000".to_string(),
    //             "20".to_string(),
    //             "aaaaaaaa".to_string(),
    //             "3".to_string(),
    //             "4".to_string(),
    //             "5".to_string(),
    //             "6".to_string(),
    //             "7".to_string(),
    //             "x".to_string(),
    //             "8".to_string(),
    //             "3.14159265358".to_string(),
    //         ]
    //     )
    // }
}
