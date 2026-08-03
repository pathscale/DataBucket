mod enum_generator;
mod generator;
mod parser;

use proc_macro2::TokenStream;
use quote::quote;

use crate::size_measure::enum_generator::EnumGenerator;
use crate::size_measure::generator::Generator;
use crate::size_measure::parser::{ParsedItem, Parser};

pub fn expand(input: &TokenStream) -> syn::Result<TokenStream> {
    let impl_def = match Parser::parse(input)? {
        ParsedItem::Struct(struct_def) => Generator { struct_def }.gen_impl(),
        ParsedItem::Enum(enum_def) => EnumGenerator { enum_def }.gen_impl()?,
    };

    Ok(quote! {
        #impl_def
    })
}
