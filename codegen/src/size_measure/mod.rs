mod generator;
mod parser;

use proc_macro2::TokenStream;
use quote::quote;

use crate::size_measure::generator::{EnumGenerator, StructGenerator};
use crate::size_measure::parser::Parser;

pub fn expand(input: &TokenStream) -> syn::Result<TokenStream> {
    if let Ok(input_fn) = Parser::parse_struct(input) {
        let gen = StructGenerator {
            struct_def: input_fn,
        };
        let impl_def = gen.gen_impl();

        return Ok(quote! {
            #impl_def
        });
    }
    if let Ok(input_fn) = Parser::parse_enum(input) {
        let gen = EnumGenerator { enum_def: input_fn };
        let impl_def = gen.gen_impl();

        Ok(quote! {
            #impl_def
        })
    } else {
        Err(Parser::parse_enum(input)
            .err()
            .expect("should be error as checked before"))
    }
}
