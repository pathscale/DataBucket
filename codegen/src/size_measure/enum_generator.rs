use proc_macro2::TokenStream;
use quote::quote;
use syn::{Fields, ItemEnum};

pub struct EnumGenerator {
    pub enum_def: ItemEnum,
}

impl EnumGenerator {
    pub fn gen_impl(&self) -> syn::Result<TokenStream> {
        if !self.enum_def.generics.params.is_empty() {
            return Err(syn::Error::new_spanned(
                &self.enum_def.generics,
                "SizeMeasure does not yet support generic enums",
            ));
        }

        if let Some(variant) = self
            .enum_def
            .variants
            .iter()
            .find(|variant| !matches!(variant.fields, Fields::Unit))
        {
            return Err(syn::Error::new_spanned(
                variant,
                "SizeMeasure supports only fieldless enums; payload sizes may be data-dependent",
            ));
        }

        let enum_ident = &self.enum_def.ident;
        Ok(quote! {
            impl SizeMeasurable for #enum_ident
            where
                #enum_ident: rkyv::Archive,
                <#enum_ident as rkyv::Archive>::Archived: Sized,
            {
                fn aligned_size(&self) -> usize {
                    std::mem::size_of::<<#enum_ident as rkyv::Archive>::Archived>()
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::EnumGenerator;
    use syn::parse_quote;

    #[test]
    fn rejects_payload_bearing_enums() {
        let enum_def = parse_quote! {
            enum Payload {
                Empty,
                Value(u64),
            }
        };

        let error = EnumGenerator { enum_def }.gen_impl().unwrap_err();
        assert!(error.to_string().contains("fieldless enums"));
    }
}
