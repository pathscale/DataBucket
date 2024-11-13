mod persist_index;
mod persist_table;

use proc_macro::TokenStream;

#[proc_macro_derive(PersistIndex)]
pub fn persist_index(input: TokenStream) -> TokenStream {
    persist_index::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}
