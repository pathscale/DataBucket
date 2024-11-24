use crate::persist_table::generator::Generator;
use crate::persist_table::parser::Parser;
use proc_macro2::TokenStream;
use quote::quote;
use syn::spanned::Spanned;

mod generator;
mod parser;

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let input_fn = Parser::parse_struct(input)?;
    let index_ident = Parser::parse_index_ident(&input_fn);
    let pk_ident = Parser::parse_pk_ident(&input_fn);

    let gen = Generator {
        struct_def: input_fn,
        pk_ident,
        index_ident,
    };

    let space_type = gen.gen_space_type()?;
    let space_impl = gen.gen_space_impls()?;
    let size_measurable_impl = gen.gen_size_measurable_impl()?;

    Ok(quote! {
        #size_measurable_impl

        #space_type
        #space_impl
    })
}
