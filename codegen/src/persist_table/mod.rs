use proc_macro2::TokenStream;
use quote::quote;
use syn::spanned::Spanned;

use crate::persist_table::parser::Parser;

mod parser;
mod generator;

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let input_fn = Parser::parse_struct(input)?;
    let index_ident = Parser::parse_index_ident(&input_fn);
    let pk_ident = Parser::parse_pk_ident(&input_fn);

    Ok(quote! {

    })
}