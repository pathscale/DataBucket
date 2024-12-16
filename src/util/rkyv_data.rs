use rkyv::{primitive::{ArchivedI128, ArchivedI16, ArchivedI32, ArchivedI64, ArchivedU128, ArchivedU16, ArchivedU32, ArchivedU64}, string::ArchivedString};

pub fn parse_archived_row(buf: &[u8], columns: Vec<(String, String)>) -> Vec<String> {
    let mut data_length: usize = {
        let mut accum: usize = 0;
        for column in columns.iter() {
            match column.1.as_str() {
                "String" => {
                    if accum % 4 != 0 {
                        accum += 4 - accum % 4;
                    }
                    accum += std::mem::size_of::<ArchivedString>();
                },

                "i128" => {
                    if accum % std::mem::size_of::<ArchivedI128>() != 0 {
                        accum += std::mem::size_of::<ArchivedI128>() - accum % std::mem::size_of::<ArchivedI128>();
                    }
                    accum += std::mem::size_of::<ArchivedI128>();
                },
                "i64" => {
                    if accum % std::mem::size_of::<ArchivedI64>() != 0 {
                        accum += std::mem::size_of::<ArchivedI64>() - accum % std::mem::size_of::<ArchivedI64>();
                    }
                    accum += std::mem::size_of::<ArchivedI64>();
                }
                "i32" => {
                    if accum % std::mem::size_of::<ArchivedI32>() != 0 {
                        accum += std::mem::size_of::<ArchivedI32>() - accum % std::mem::size_of::<ArchivedI32>();
                    }
                    accum += std::mem::size_of::<ArchivedI32>();
                }
                "i16" => {
                    if accum % std::mem::size_of::<ArchivedI16>() != 0 {
                        accum += std::mem::size_of::<ArchivedI16>() - accum % std::mem::size_of::<ArchivedI16>();
                    }
                    accum += std::mem::size_of::<ArchivedI16>();
                }
                "i8" => accum += std::mem::size_of::<i8>(),

                "u128" => {
                    if accum % std::mem::size_of::<ArchivedU128>() != 0 {
                        accum += std::mem::size_of::<ArchivedU128>() - accum % std::mem::size_of::<ArchivedU128>();
                    }
                    accum += std::mem::size_of::<ArchivedU128>();
                }
                "u64" => {
                    if accum % std::mem::size_of::<ArchivedU64>() != 0 {
                        accum += std::mem::size_of::<ArchivedU64>() - accum % std::mem::size_of::<ArchivedU64>();
                    }
                    accum += std::mem::size_of::<ArchivedU64>();
                }
                "u32" => {
                    if accum % std::mem::size_of::<ArchivedU32>() != 0 {
                        accum += std::mem::size_of::<ArchivedU32>() - accum % std::mem::size_of::<ArchivedU32>();
                    }
                    accum += std::mem::size_of::<ArchivedU32>();
                }
                "u16" => {
                    if accum % std::mem::size_of::<ArchivedU16>() != 0 {
                        accum += std::mem::size_of::<ArchivedU16>() - accum % std::mem::size_of::<ArchivedU16>();
                    }
                    accum += std::mem::size_of::<ArchivedU16>();
                }
                "u8" => accum += std::mem::size_of::<u8>(),

                _ => panic!("Unknown data type {:?}", column.1),
            }
        }
        accum
    };
    if data_length % 4 != 0 {
        data_length += 4 - data_length % 4;
    }

    let start_pointer = unsafe { buf.as_ptr().add(buf.len()).sub(data_length) };
    let mut current_pointer = start_pointer;
    let mut output: Vec<String> = vec![];
    for column in columns.iter() {
        match column.1.as_str() {
            "String" => {
                if unsafe { current_pointer.byte_offset_from(start_pointer) } % 4 != 0 {
                    current_pointer = unsafe { current_pointer.add((4 - current_pointer.byte_offset_from(start_pointer) % 4) as usize) };
                }
                let archived_ptr: *const ArchivedString = current_pointer.cast();
                output.push(unsafe { (*archived_ptr).to_string() });
                current_pointer = unsafe { current_pointer.add(std::mem::size_of::<ArchivedString>()) };
            },

            "i128" => {
                if unsafe { current_pointer.byte_offset_from(start_pointer) % std::mem::size_of::<ArchivedI128>() as isize != 0 } {
                    current_pointer = unsafe { current_pointer.add(
                        (std::mem::size_of::<ArchivedI128>() as isize -
                        current_pointer.byte_offset_from(start_pointer) % std::mem::size_of::<ArchivedI128>() as isize) as usize) };
                }
                let archived_ptr: *const ArchivedI128 = current_pointer.cast();
                output.push(unsafe { (*archived_ptr).to_string() });
                current_pointer = unsafe { current_pointer.add(std::mem::size_of::<ArchivedI128>()) };
            },
            "i64" => {
                if unsafe { current_pointer.byte_offset_from(start_pointer) % std::mem::size_of::<ArchivedI64>() as isize != 0 } {
                    current_pointer = unsafe { current_pointer.add(
                        (std::mem::size_of::<ArchivedI64>() as isize -
                        current_pointer.byte_offset_from(start_pointer) % std::mem::size_of::<ArchivedI64>() as isize) as usize) };
                }
                let archived_ptr: *const ArchivedI64 = current_pointer.cast();
                output.push(unsafe { (*archived_ptr).to_string() });
                current_pointer = unsafe { current_pointer.add(std::mem::size_of::<ArchivedI64>()) };
            },
            "i32" => {
                if unsafe { current_pointer.byte_offset_from(start_pointer) % std::mem::size_of::<ArchivedI32>() as isize != 0 } {
                    current_pointer = unsafe { current_pointer.add(
                        (std::mem::size_of::<ArchivedI32>() as isize -
                        current_pointer.byte_offset_from(start_pointer) % std::mem::size_of::<ArchivedI32>() as isize) as usize) };
                }
                let archived_ptr: *const ArchivedI32 = current_pointer.cast();
                output.push(unsafe { (*archived_ptr).to_string() });
                current_pointer = unsafe { current_pointer.add(std::mem::size_of::<ArchivedI32>()) };
            },
            "i16" => {
                if unsafe { current_pointer.byte_offset_from(start_pointer) % std::mem::size_of::<ArchivedI16>() as isize != 0 } {
                    current_pointer = unsafe { current_pointer.add(
                        (std::mem::size_of::<ArchivedI16>() as isize -
                        current_pointer.byte_offset_from(start_pointer) % std::mem::size_of::<ArchivedI16>() as isize) as usize) };
                }
                let archived_ptr: *const ArchivedI16 = current_pointer.cast();
                output.push(unsafe { (*archived_ptr).to_string() });
                current_pointer = unsafe { current_pointer.add(std::mem::size_of::<ArchivedI16>()) };
            },
            "i8" => {
                let archived_ptr: *const i8 = current_pointer.cast();
                output.push(unsafe { (*archived_ptr).to_string() });
                current_pointer = unsafe { current_pointer.add(std::mem::size_of::<i8>()) };
            },

            "u128" => {
                if unsafe { current_pointer.byte_offset_from(start_pointer) % std::mem::size_of::<ArchivedU128>() as isize != 0 } {
                    current_pointer = unsafe { current_pointer.add(
                        (std::mem::size_of::<ArchivedU128>() as isize -
                        current_pointer.byte_offset_from(start_pointer) % std::mem::size_of::<ArchivedU128>() as isize) as usize) };
                }
                let archived_ptr: *const ArchivedU128 = current_pointer.cast();
                output.push(unsafe { (*archived_ptr).to_string() });
                current_pointer = unsafe { current_pointer.add(std::mem::size_of::<ArchivedU128>()) };
            },
            "u64" => {
                if unsafe { current_pointer.byte_offset_from(start_pointer) % std::mem::size_of::<ArchivedU64>() as isize != 0 } {
                    current_pointer = unsafe { current_pointer.add(
                        (std::mem::size_of::<ArchivedU64>() as isize -
                        current_pointer.byte_offset_from(start_pointer) % std::mem::size_of::<ArchivedU64>() as isize) as usize) };
                }
                let archived_ptr: *const ArchivedU64 = current_pointer.cast();
                output.push(unsafe { (*archived_ptr).to_string() });
                current_pointer = unsafe { current_pointer.add(std::mem::size_of::<ArchivedU64>()) };
            },
            "u32" => {
                if unsafe { current_pointer.byte_offset_from(start_pointer) % std::mem::size_of::<ArchivedU32>() as isize != 0 } {
                    current_pointer = unsafe { current_pointer.add(
                        (std::mem::size_of::<ArchivedU32>() as isize -
                        current_pointer.byte_offset_from(start_pointer) % std::mem::size_of::<ArchivedU32>() as isize) as usize) };
                }
                let archived_ptr: *const ArchivedU32 = current_pointer.cast();
                output.push(unsafe { (*archived_ptr).to_string() });
                current_pointer = unsafe { current_pointer.add(std::mem::size_of::<ArchivedU32>()) };
            },
            "u16" => {
                if unsafe { current_pointer.byte_offset_from(start_pointer) % std::mem::size_of::<ArchivedI16>() as isize != 0 } {
                    current_pointer = unsafe { current_pointer.add(
                        (std::mem::size_of::<ArchivedI16>() as isize -
                        current_pointer.byte_offset_from(start_pointer) % std::mem::size_of::<ArchivedI16>() as isize) as usize) };
                }
                let archived_ptr: *const ArchivedU16 = current_pointer.cast();
                output.push(unsafe { (*archived_ptr).to_string() });
                current_pointer = unsafe { current_pointer.add(std::mem::size_of::<ArchivedU16>()) };
            },
            "u8" => {
                let archived_ptr: *const u8 = current_pointer.cast();
                output.push(unsafe { (*archived_ptr).to_string() });
                current_pointer = unsafe { current_pointer.add(std::mem::size_of::<u8>()) };
            },

            _ => panic!("Unknown data type: {:?}", column.1),
        }
    }
    output
}

#[cfg(test)]
mod test {
    use rkyv::{Archive, Deserialize, Serialize};

    use super::parse_archived_row;

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
        pub string7: String,
    }

    #[test]
    fn test_struct() {
        let buffer = rkyv::to_bytes::<rkyv::rancor::Error>(&Struct {
            string1: "000000000000000".to_string(),
            int1: 20,
            string2: "aaaaaaaa".to_string(),
            int2: 3,
            int3: 4,
            int4: 5,
            int5: 6,
            int6: 7,
            string7: "x".to_owned(),
        }).unwrap();
        let parsed = parse_archived_row(&buffer, vec![
            ("string1".to_string(), "String".to_string()),
            ("int1".to_string(), "i32".to_string()),
            ("string2".to_string(), "String".to_string()),
            ("int2".to_string(), "u8".to_string()),
            ("int3".to_string(), "i8".to_string()),
            ("int4".to_string(), "u8".to_string()),
            ("int5".to_string(), "i32".to_string()),
            ("int6".to_string(), "u8".to_string()),
            ("string7".to_string(), "String".to_string()),
        ]);
        assert_eq!(parsed, [
            "000000000000000".to_string(),
            "20".to_string(),
            "aaaaaaaa".to_string(),
            "3".to_string(),
            "4".to_string(),
            "5".to_string(),
            "6".to_string(),
            "7".to_string(),
            "x".to_string(),
        ])
    }
}
