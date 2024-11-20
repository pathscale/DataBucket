use proc_macro2::{Ident, Literal, TokenStream};
use quote::__private::Span;
use quote::quote;

use crate::persist_table::generator::Generator;

impl Generator {
    pub fn gen_deserialize(&self) -> syn::Result<TokenStream> {
        let name = self.struct_def.ident.to_string().replace("WorkTable", "");
        let space_ident = Ident::new(format!("{}Space", name).as_str(), Span::mixed_site());

        let parse_info = self.gen_parse_info_fn()?;

        Ok(quote! {
            impl<const DATA_LENGTH: usize> #space_ident<DATA_LENGTH> {
                #parse_info
            }
        })
    }

    fn gen_parse_info_fn(&self) -> syn::Result<TokenStream> {
        let name = self.struct_def.ident.to_string().replace("WorkTable", "");
        let const_name = Ident::new(
            format!("{}_PAGE_SIZE", name.to_uppercase()).as_str(),
            Span::mixed_site(),
        );

        Ok(quote! {
            pub fn parse_info(file: &mut std::fs::File) -> eyre::Result<GeneralPage<SpaceInfoData>> {
                use std::io;
                use std::io::prelude::*;
                use rkyv::Deserialize;

                let mut buffer = [0; HEADER_LENGTH];
                file.read(&mut buffer)?;
                let archived = unsafe { rkyv::archived_root::<GeneralHeader>(&buffer[..]) };
                let mut map = rkyv::de::deserializers::SharedDeserializeMap::new();
                let header = archived.deserialize(&mut map)?;

                let mut buffer = [0; #const_name - HEADER_LENGTH];
                file.read(&mut buffer)?;
                let archived = unsafe { rkyv::archived_root::<SpaceInfoData>(&buffer[..]) };
                let mut map = rkyv::de::deserializers::SharedDeserializeMap::new();
                let info = archived.deserialize(&mut map)?;

                Ok(
                    GeneralPage {
                        header,
                        inner: info
                    }
                )
            }
        })
    }
}