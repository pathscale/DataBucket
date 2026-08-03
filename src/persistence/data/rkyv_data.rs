use crate::persistence::data::types::DataTypeValue;
use std::str::FromStr;

pub fn parse_archived_row<S1: AsRef<str>, S2: AsRef<str>>(
    buf: &[u8],
    columns: &[(S1, S2)],
) -> Vec<DataTypeValue> {
    if columns.is_empty() {
        return Vec::new();
    }

    let mut data_length: usize = {
        let mut accum: usize = 0;
        for column in columns.iter() {
            let value =
                DataTypeValue::from_str(column.1.as_ref()).expect("data type should be supported");
            let data_type = value.as_data_type();
            data_type.advance_accum(&mut accum);
        }
        accum
    };
    if !data_length.is_multiple_of(4) {
        data_length += 4 - data_length % 4;
    }

    let row_start = buf.len().checked_sub(data_length).unwrap_or_else(|| {
        panic!(
            "archived row buffer is too short: schema requires {data_length} bytes, buffer has {}",
            buf.len()
        )
    });
    let mut row_offset = 0usize;
    let mut output: Vec<_> = vec![];
    for column in columns.iter() {
        let value =
            DataTypeValue::from_str(column.1.as_ref()).expect("data type should be supported");
        let data_type = value.as_data_type();
        data_type.advance_accum(&mut row_offset);
        let field_end = row_start
            .checked_add(row_offset)
            .filter(|field_end| *field_end <= buf.len())
            .expect("archived row field extends beyond the buffer");
        let deserialized = data_type.from_archived_bytes(&buf[..field_end]);
        output.push(deserialized);
    }
    output
}

#[cfg(test)]
mod test {
    use super::parse_archived_row;
    use crate::persistence::data::types::DataTypeValue;
    use rkyv::{Archive, Deserialize, Serialize};
    use std::f64::consts::PI;

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
        let parsed = parse_archived_row(&buffer, &[("string1", "String")]);
        assert_eq!(
            parsed,
            [DataTypeValue::String("000000000000000".to_string())]
        )
    }

    #[test]
    fn empty_schema_accepts_empty_buffer() {
        let columns: [(String, String); 0] = [];
        assert!(parse_archived_row(&[], &columns).is_empty());
    }

    #[test]
    #[should_panic(
        expected = "archived row buffer is too short: schema requires 8 bytes, buffer has 0"
    )]
    fn short_buffer_panics_before_pointer_arithmetic() {
        let _ = parse_archived_row(&[], &[("col", "String")]);
    }

    #[test]
    #[should_panic(expected = "archived row string field is invalid")]
    fn malformed_string_archive_is_rejected() {
        // An out-of-line string of length 9 whose relative pointer is far
        // outside this eight-byte buffer.
        let malformed = [0x89, 0, 0, 0, 0x7f, 0x7f, 0x7f, 0x7f];
        let _ = parse_archived_row(&malformed, &[("col", "String")]);
    }

    #[derive(Archive, Serialize, Deserialize, Debug)]
    struct Struct2 {
        pub int1: i32,
    }

    #[test]
    fn test_parse_archived_row_int() {
        let buffer = rkyv::to_bytes::<rkyv::rancor::Error>(&Struct2 { int1: 3 }).unwrap();
        let parsed = parse_archived_row(&buffer, &[("int1", "i32")]);
        assert_eq!(parsed, [DataTypeValue::I32(3)])
    }

    #[test]
    #[should_panic(expected = "archived row primitive field is invalid")]
    fn misaligned_primitive_archive_is_rejected() {
        let buffer = rkyv::to_bytes::<rkyv::rancor::Error>(&Struct2 { int1: 3 }).unwrap();
        let mut storage = Vec::with_capacity(buffer.len() + 1);
        storage.push(0);
        storage.extend_from_slice(&buffer);

        let _ = parse_archived_row(&storage[1..], &[("int1", "i32")]);
    }

    #[derive(Archive, Serialize, Deserialize, Debug)]
    struct Struct3 {
        pub float1: f64,
    }

    #[test]
    fn test_parse_archived_row_float() {
        let buffer = rkyv::to_bytes::<rkyv::rancor::Error>(&Struct3 { float1: PI }).unwrap();
        let parsed = parse_archived_row(&buffer, &[("float1", "f64")]);
        assert_eq!(parsed, [DataTypeValue::F64(PI)])
    }

    #[derive(Archive, Serialize, Deserialize, Debug)]
    struct Struct4 {
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

    #[test]
    fn test_parse_archived_row_many_fields() {
        let buffer = rkyv::to_bytes::<rkyv::rancor::Error>(&Struct4 {
            string1: "000000000000000".to_string(),
            int1: 20,
            string2: "aaaaaaaa".to_string(),
            int2: 3,
            int3: 4,
            int4: 5,
            int5: 6,
            int6: 7,
            string3: "x".to_string(),
            int7: 8,
            float1: PI,
        })
        .unwrap();
        let parsed = parse_archived_row(
            &buffer,
            &[
                ("string1".to_string(), "String".to_string()),
                ("int1".to_string(), "i32".to_string()),
                ("string2".to_string(), "String".to_string()),
                ("int2".to_string(), "u8".to_string()),
                ("int3".to_string(), "i8".to_string()),
                ("int4".to_string(), "u8".to_string()),
                ("int5".to_string(), "i32".to_string()),
                ("int6".to_string(), "u8".to_string()),
                ("string3".to_string(), "String".to_string()),
                ("int7".to_string(), "i8".to_string()),
                ("float1".to_string(), "f64".to_string()),
            ],
        );
        assert_eq!(
            parsed,
            [
                DataTypeValue::String("000000000000000".to_string()),
                DataTypeValue::I32(20),
                DataTypeValue::String("aaaaaaaa".to_string()),
                DataTypeValue::U8(3),
                DataTypeValue::I8(4),
                DataTypeValue::U8(5),
                DataTypeValue::I32(6),
                DataTypeValue::U8(7),
                DataTypeValue::String("x".to_string()),
                DataTypeValue::I8(8),
                DataTypeValue::F64(PI),
            ]
        )
    }
}
