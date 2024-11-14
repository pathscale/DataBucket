use proc_macro2::{Ident, Span, TokenStream};
use quote::ToTokens;
use syn::spanned::Spanned;
use syn::ItemStruct;

pub struct Parser;

impl Parser {
    pub fn parse_struct(input: TokenStream) -> syn::Result<ItemStruct> {
        match syn::parse2::<ItemStruct>(input.clone()) {
            Ok(data) => Ok(data),
            Err(err) => Err(syn::Error::new(input.span(), err.to_string())),
        }
    }

    pub fn parse_pk_ident(item: &ItemStruct) -> Ident {
        /// WorkTable<#row_type, #pk_type, #index_type, <#pk_type as TablePrimaryKey>::Generator, #const_name>
        let type_str = item
            .fields
            .iter()
            .next()
            .unwrap()
            .ty
            .to_token_stream()
            .to_string();
        let mut split = type_str.split("<");
        split.next();
        let gens = split.next().unwrap().split(",");
        let index = gens.skip(1).next().unwrap();

        Ident::new(index.trim(), Span::mixed_site())
    }

    pub fn parse_index_ident(item: &ItemStruct) -> Ident {
        /// WorkTable<#row_type, #pk_type, #index_type, <#pk_type as TablePrimaryKey>::Generator, #const_name>
        let type_str = item
            .fields
            .iter()
            .next()
            .unwrap()
            .ty
            .to_token_stream()
            .to_string();
        let mut split = type_str.split("<");
        split.next();
        let gens = split.next().unwrap().split(",");
        let index = gens.skip(2).next().unwrap().replace(">", "");

        Ident::new(index.trim(), Span::mixed_site())
    }
}
