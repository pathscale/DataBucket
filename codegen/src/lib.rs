mod persistable;
mod size_measure;

use proc_macro::TokenStream;

#[proc_macro_derive(SizeMeasure)]
pub fn size_measure(input: TokenStream) -> TokenStream {
    size_measure::expand(&(input.into()))
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

#[proc_macro_derive(Persistable, attributes(persistable))]
pub fn persistable(input: TokenStream) -> TokenStream {
    persistable::expand(&(input.into()))
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}
