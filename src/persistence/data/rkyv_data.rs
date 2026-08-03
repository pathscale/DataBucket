use crate::persistence::data::{DataDecodeError, DataTypeValue};
use std::str::FromStr;

/// Decodes a dynamically described row from an rkyv archive.
///
/// # Errors
///
/// Returns [`DataDecodeError`] when the schema names an unsupported type, the
/// fixed-width row root does not fit in `buf`, or any archived field fails
/// rkyv validation. Malformed persisted data never enters an unchecked access
/// path.
pub fn parse_archived_row<S1: AsRef<str>, S2: AsRef<str>>(
    buf: &[u8],
    columns: &[(S1, S2)],
) -> Result<Vec<DataTypeValue>, DataDecodeError> {
    if columns.is_empty() {
        return Ok(Vec::new());
    }

    let data_types = columns
        .iter()
        .map(|column| DataTypeValue::from_str(column.1.as_ref()))
        .collect::<Result<Vec<_>, _>>()?;
    let mut data_length = 0usize;
    for value in &data_types {
        value.as_data_type().advance_accum(&mut data_length);
    }
    if !data_length.is_multiple_of(4) {
        data_length += 4 - data_length % 4;
    }

    let row_start = buf
        .len()
        .checked_sub(data_length)
        .ok_or(DataDecodeError::BufferTooShort {
            required: data_length,
            actual: buf.len(),
        })?;
    let mut row_offset = 0usize;
    let mut output = Vec::with_capacity(data_types.len());
    for (field_index, value) in data_types.iter().enumerate() {
        let data_type = value.as_data_type();
        data_type.advance_accum(&mut row_offset);
        let field_end = row_start
            .checked_add(row_offset)
            .filter(|field_end| *field_end <= buf.len())
            .ok_or(DataDecodeError::FieldOutOfBounds {
                field_index,
                field_end: row_start.saturating_add(row_offset),
                actual: buf.len(),
            })?;

        // `rkyv::access` roots the requested archived value at the end of its
        // input slice. The slice must therefore end exactly after this field,
        // while retaining the complete serialized prefix: an out-of-line
        // archived string may point backward into payload bytes before the
        // fixed-width row root at `row_start`.
        let deserialized = data_type.from_archived_bytes(&buf[..field_end])?;
        output.push(deserialized);
    }
    Ok(output)
}

#[cfg(test)]
mod test {
    use super::parse_archived_row;
    use crate::persistence::data::{DataDecodeError, DataTypeValue};
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
        let parsed = parse_archived_row(&buffer, &[("string1", "String")]).unwrap();
        assert_eq!(
            parsed,
            [DataTypeValue::String("000000000000000".to_string())]
        )
    }

    #[test]
    fn empty_schema_accepts_empty_buffer() {
        let columns: [(String, String); 0] = [];
        assert!(parse_archived_row(&[], &columns).unwrap().is_empty());
    }

    #[test]
    fn short_buffer_returns_error_before_decoding() {
        assert_eq!(
            parse_archived_row(&[], &[("col", "String")]),
            Err(DataDecodeError::BufferTooShort {
                required: 8,
                actual: 0,
            })
        );
    }

    #[test]
    fn malformed_string_archive_is_rejected() {
        // An out-of-line string of length 9 whose relative pointer is far
        // outside this eight-byte buffer.
        let malformed = [0x89, 0, 0, 0, 0x7f, 0x7f, 0x7f, 0x7f];
        assert!(matches!(
            parse_archived_row(&malformed, &[("col", "String")]),
            Err(DataDecodeError::InvalidArchive {
                data_type: "String",
                ..
            })
        ));
    }

    #[test]
    fn unsupported_schema_type_is_rejected() {
        assert_eq!(
            parse_archived_row(&[0; 8], &[("col", "not-a-type")]),
            Err(DataDecodeError::UnsupportedDataType {
                data_type: "not-a-type".to_owned(),
            })
        );
    }

    #[derive(Archive, Serialize, Deserialize, Debug)]
    struct Struct2 {
        pub int1: i32,
    }

    #[test]
    fn test_parse_archived_row_int() {
        let buffer = rkyv::to_bytes::<rkyv::rancor::Error>(&Struct2 { int1: 3 }).unwrap();
        let parsed = parse_archived_row(&buffer, &[("int1", "i32")]).unwrap();
        assert_eq!(parsed, [DataTypeValue::I32(3)])
    }

    #[test]
    fn misaligned_primitive_archive_is_rejected() {
        let buffer = rkyv::to_bytes::<rkyv::rancor::Error>(&Struct2 { int1: 3 }).unwrap();
        let mut storage = Vec::with_capacity(buffer.len() + 1);
        storage.push(0);
        storage.extend_from_slice(&buffer);

        assert!(matches!(
            parse_archived_row(&storage[1..], &[("int1", "i32")]),
            Err(DataDecodeError::InvalidArchive {
                data_type: "i32",
                ..
            })
        ));
    }

    #[derive(Archive, Serialize, Deserialize, Debug)]
    struct Struct3 {
        pub float1: f64,
    }

    #[test]
    fn test_parse_archived_row_float() {
        let buffer = rkyv::to_bytes::<rkyv::rancor::Error>(&Struct3 { float1: PI }).unwrap();
        let parsed = parse_archived_row(&buffer, &[("float1", "f64")]).unwrap();
        assert_eq!(parsed, [DataTypeValue::F64(PI)])
    }

    #[derive(Archive, Serialize, Deserialize, Debug)]
    struct StructWithTrailingLongString {
        pub int1: u32,
        pub string1: String,
    }

    #[test]
    fn out_of_line_string_after_primitive_uses_complete_archive_prefix() {
        let string1 = "out-of-line string payload after a primitive".to_owned();
        let buffer = rkyv::to_bytes::<rkyv::rancor::Error>(&StructWithTrailingLongString {
            int1: 42,
            string1: string1.clone(),
        })
        .unwrap();

        // The payload precedes the fixed-width archived row root. Keeping the
        // full prefix is required for the string's relative pointer to remain
        // within the validation subtree.
        assert!(buffer.len() > 12);
        assert_eq!(
            parse_archived_row(&buffer, &[("int1", "u32"), ("string1", "String")]).unwrap(),
            [DataTypeValue::U32(42), DataTypeValue::String(string1)]
        );
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
        )
        .unwrap();
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
