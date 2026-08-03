pub fn advance_accum_for_padding(mut accum: usize, padding: usize) -> usize {
    if !accum.is_multiple_of(padding) {
        accum += padding - accum % padding;
    }
    accum
}
