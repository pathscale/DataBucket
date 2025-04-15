use crate::persistable::generator::PersistableAttributes;
use proc_macro2::TokenStream;
use quote::ToTokens;
use syn::spanned::Spanned;
use syn::{Attribute, ItemStruct};

pub struct Parser;

impl Parser {
    pub fn parse_struct(input: &TokenStream) -> syn::Result<ItemStruct> {
        match syn::parse2::<ItemStruct>(input.clone()) {
            Ok(data) => Ok(data),
            Err(err) => Err(syn::Error::new(input.span(), err.to_string())),
        }
    }

    pub fn parse_attributes(attrs: &Vec<Attribute>) -> PersistableAttributes {
        let mut res = PersistableAttributes {
            is_full_row: true,
            is_generic_unsized: false,
        };

        for attr in attrs {
            if attr.path().to_token_stream().to_string().as_str() == "persistable" {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("by_parts") {
                        res.is_full_row = false;
                        return Ok(());
                    }
                    if meta.path.is_ident("unsized_gens") {
                        res.is_generic_unsized = true;
                        return Ok(());
                    }
                    Ok(())
                })
                .expect("always ok even on unrecognized attrs");
            }
        }

        res
    }
}
