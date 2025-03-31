mod persistable_impl;

use proc_macro2::TokenStream;
use quote::quote;
use syn::ItemStruct;

pub struct Generator {
    pub is_full_row: bool,
    pub struct_def: ItemStruct,
}

pub struct PersistableAttributes {
    pub is_full_row: bool,
}

impl Generator {
    pub fn gen_def(&self) -> TokenStream {
        let persistable_impl = self.gen_perisistable_impl();
        quote! {
            #persistable_impl
        }
    }
}
