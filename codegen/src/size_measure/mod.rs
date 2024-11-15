mod parser;
mod generator;

use proc_macro2::TokenStream;
use quote::quote;

use crate::size_measure::generator::Generator;
use crate::size_measure::parser::Parser;

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let input_fn = Parser::parse_struct(input)?;
    let mut gen = Generator {
        struct_def: input_fn
    };

    let impl_def = gen.gen_impl()?;

    Ok(quote! {
        #impl_def
    })
}