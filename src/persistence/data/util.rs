pub fn advance_accum_for_padding(mut accum: usize, padding: usize) -> usize {
    if accum % padding != 0 {
        accum += padding - accum % padding;
    }
    accum
}

pub fn advance_pointer_for_padding(
    mut current_pointer: *const u8,
    start_pointer: *const u8,
    padding: usize,
) -> *const u8 {
    if unsafe { current_pointer.byte_offset_from(start_pointer) % padding as isize != 0 } {
        current_pointer = unsafe {
            current_pointer.add(
                (padding as isize
                    - current_pointer.byte_offset_from(start_pointer) % padding as isize)
                    as usize,
            )
        };
    }
    current_pointer
}
