mod persist_index;
mod persist_table;
mod size_measure;

use proc_macro::TokenStream;

#[proc_macro_derive(PersistIndex)]
pub fn persist_index(input: TokenStream) -> TokenStream {
    persist_index::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

#[proc_macro_derive(PersistTable)]
pub fn persist_table(input: TokenStream) -> TokenStream {
    persist_table::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

#[proc_macro_derive(SizeMeasure)]
pub fn size_measure(input: TokenStream) -> TokenStream {
    size_measure::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}