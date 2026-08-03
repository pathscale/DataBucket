use proc_macro2::TokenStream;
use syn::spanned::Spanned;
use syn::{Item, ItemEnum, ItemStruct};

pub enum ParsedItem {
    Struct(ItemStruct),
    Enum(ItemEnum),
}

pub struct Parser;

impl Parser {
    pub fn parse(input: &TokenStream) -> syn::Result<ParsedItem> {
        match syn::parse2::<Item>(input.clone()) {
            Ok(Item::Struct(data)) => Ok(ParsedItem::Struct(data)),
            Ok(Item::Enum(data)) => Ok(ParsedItem::Enum(data)),
            Ok(item) => Err(syn::Error::new_spanned(
                item,
                "SizeMeasure supports structs and fieldless enums",
            )),
            Err(err) => Err(syn::Error::new(input.span(), err.to_string())),
        }
    }
}
