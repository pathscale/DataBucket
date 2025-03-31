use crate::persistable::generator::Generator;
use proc_macro2::TokenStream;
use quote::{quote, ToTokens};
use syn::GenericParam;

impl Generator {
    pub fn gen_perisistable_impl(&self) -> TokenStream {
        let struct_ident = &self.struct_def.ident;
        let generics = &self.struct_def.generics;
        let trait_bounds: Vec<_> = generics.params.iter().map(|generic| {
            if let GenericParam::Type(param) = generic {
                let ident = &param.ident;
                let archived_bounds = if param.bounds.to_token_stream().to_string().contains("Ord") {
                    quote! {
                        + Ord
                    }
                } else {
                    quote! {}
                };
                quote! {
                        #ident: rkyv::Archive
                            + for<'a> rkyv::Serialize<
                                rkyv::rancor::Strategy<
                                    rkyv::ser::Serializer<rkyv::util::AlignedVec, rkyv::ser::allocator::ArenaHandle<'a>, rkyv::ser::sharing::Share>,
                                    rkyv::rancor::Error>,
                            >,
                        <#ident as rkyv::Archive>::Archived: rkyv::Deserialize<#ident, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>> #archived_bounds,
                }
            } else {
                quote! {}
            }
        }).collect();
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
        let where_clause = if let Some(where_clause) = where_clause {
            quote! {
                #where_clause
                    #(#trait_bounds)*
            }
        } else {
            quote! {
                where
                    #(#trait_bounds)*
            }
        };

        quote! {
            impl #impl_generics Persistable for #struct_ident #ty_generics
            #where_clause
            {
                fn as_bytes(&self) -> impl AsRef<[u8]> {
                    rkyv::to_bytes::<rkyv::rancor::Error>(self).unwrap()
                }

                fn from_bytes(bytes: &[u8]) -> Self {
                    let archived = unsafe { rkyv::access_unchecked::<<Self as Archive>::Archived>(bytes) };
                    rkyv::deserialize::<_, rkyv::rancor::Error>(archived).expect("data should be valid")
                }
            }
        }
    }
}
