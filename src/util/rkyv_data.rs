use rkyv::{primitive::ArchivedI32, string::ArchivedString};

pub fn parse_archived_row(buf: &[u8], columns: Vec<(String, String)>) -> Vec<String> {
    let data_length: usize = {
        let mut accum: usize = 0;
        for column in columns.iter() {
            match column.1.as_str() {
                "String" => accum += std::mem::size_of::<ArchivedString>(),
                "i32" => accum += std::mem::size_of::<ArchivedI32>(),
                _ => panic!("Unknown data type {:?}", column.1),
            }
        }
        accum
    };

    let mut data_pointer: *const u8 = unsafe { buf.as_ptr().add(buf.len()).sub(data_length) };
    let mut output: Vec<String> = vec![];
    for column in columns.iter() {
        match column.1.as_str() {
            "String" => {
                let archived_ptr: *const ArchivedString = data_pointer.cast();
                output.push(unsafe { (*archived_ptr).to_string() });
                data_pointer = unsafe { data_pointer.add(std::mem::size_of::<ArchivedString>()) };
            },
            "i32" => {
                let archived_ptr: *const ArchivedI32 = data_pointer.cast();
                output.push(unsafe { (*archived_ptr).to_string() });
                data_pointer = unsafe { data_pointer.add(std::mem::size_of::<ArchivedI32>()) };
            },
            _ => panic!("Unknown data type: {:?}", column.1),
        }
    }
    output
}