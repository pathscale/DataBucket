use crate::persistable::parser::Parser;
use proc_macro2::TokenStream;
use quote::quote;

mod generator;
mod parser;

pub fn expand(input: &TokenStream) -> syn::Result<TokenStream> {
    let input_struct = Parser::parse_struct(input)?;
    let attrs = Parser::parse_attributes(&input_struct.attrs);
    let gen = generator::Generator {
        is_full_row: attrs.is_full_row,
        struct_def: input_struct,
    };

    let def = gen.gen_def()?;

    Ok(quote! {
        #def
    })
}
