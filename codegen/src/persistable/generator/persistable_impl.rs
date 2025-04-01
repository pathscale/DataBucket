use crate::persistable::generator::Generator;
use proc_macro2::TokenStream;
use quote::{quote, ToTokens};
use syn::spanned::Spanned;
use syn::GenericParam;

pub fn is_primitive(ty: &str) -> bool {
    matches!(
        ty,
        "u8" | "u16"
            | "u32"
            | "u64"
            | "usize"
            | "i8"
            | "i16"
            | "i32"
            | "i64"
            | "isize"
            | "bool"
            | "char"
            | "f64"
            | "f32"
    )
}

impl Generator {
    /// Generates `Persistable` trait implementation. Implementation is different for full row persistence and
    /// persistence by parts.
    pub fn gen_perisistable_impl(&self) -> syn::Result<TokenStream> {
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

        let inner_part = if self.is_full_row {
            self.gen_perisistable_full()
        } else {
            self.gen_perisistable_by_parts()?
        };

        Ok(quote! {
            impl #impl_generics Persistable for #struct_ident #ty_generics
            #where_clause
            {
                #inner_part
            }
        })
    }

    /// Generates `Persistable` trait implementation for `full row` variant. It just call's `rkyv`'s serialization and
    /// deserialization logic for full object.
    fn gen_perisistable_full(&self) -> TokenStream {
        quote! {
                fn as_bytes(&self) -> impl AsRef<[u8]> {
                    rkyv::to_bytes::<rkyv::rancor::Error>(self).unwrap()
                }

                fn from_bytes(bytes: &[u8]) -> Self {
                    let archived = unsafe { rkyv::access_unchecked::<<Self as Archive>::Archived>(bytes) };
                    rkyv::deserialize::<_, rkyv::rancor::Error>(archived).expect("data should be valid")
                }
        }
    }

    /// Generates `Persistable` trait implementation for `by parts` variant. It call's `rkyv`'s serialization and
    /// deserialization logic for each field and unites these bytes into final array.
    fn gen_perisistable_by_parts(&self) -> syn::Result<TokenStream> {
        let as_bytes_fn = self.gen_perisistable_by_parts_as_bytes_fn();
        let from_bytes_fn = self.gen_perisistable_by_parts_from_bytes_fn()?;
        Ok(quote! {
            #as_bytes_fn
            #from_bytes_fn
        })
    }

    fn gen_perisistable_by_parts_as_bytes_fn(&self) -> TokenStream {
        let field_serialize: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .map(|f| {
                let ident = &f.ident.clone().expect("is not tuple struct");
                quote! {
                    let val_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&self.#ident).unwrap();
                    bytes.extend_from_slice(val_bytes.as_ref());
                }
            })
            .collect();
        quote! {
            fn as_bytes(&self) -> impl AsRef<[u8]> {
                let mut bytes = Vec::with_capacity(self.size as usize);
                #(#field_serialize)*
                bytes
            }
        }
    }

    fn gen_perisistable_by_parts_from_bytes_fn(&self) -> syn::Result<TokenStream> {
        let size_field = self
            .struct_def
            .fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.ident.clone().unwrap() == "size");
        if let Some((pos, size_field)) = size_field {
            if pos != 0 {
                return Err(syn::Error::new(
                    size_field.span(),
                    "`size` field should be first field in a struct",
                ));
            }
            let size_type = &size_field.ty;
            let field_deserialize: Vec<_> = self
                .struct_def
                .fields
                .iter()
                .filter(|f| !f.ident.clone().unwrap().to_string().contains("size"))
                .map(|f| {
                    let ident = &f.ident.clone().expect("is not tuple struct");
                    if f.ty.to_token_stream().to_string().contains("Vec") {
                        let ty = &f.ty;
                        let inner_ty_str = ty.to_token_stream().to_string().replace(" ", "");
                        let mut inner_ty_str = inner_ty_str.replace("Vec<", "");
                        inner_ty_str.pop();
                        let inner_ty: TokenStream = inner_ty_str.parse().unwrap();
                        let len = if is_primitive(&inner_ty_str) {
                            quote! {
                                let values_len = align(size as usize * <#inner_ty as Default>::default().aligned_size()) + 8;
                            }
                        } else {
                            quote! {
                                let values_len = size as usize * align8(<#inner_ty as Default>::default().aligned_size()) + 8;
                            }
                        };
                        quote! {
                            #len
                            let mut v = rkyv::util::AlignedVec::<4>::new();
                            v.extend_from_slice(&bytes[offset..offset + values_len]);
                            let archived =
                                unsafe { rkyv::access_unchecked::<<#ty as Archive>::Archived>(&v[..]) };
                            let #ident = rkyv::deserialize::<#ty, rkyv::rancor::Error>(archived)
                                .expect("data should be valid");
                            offset += values_len;
                        }
                    } else {
                        let ty = &f.ty;
                        quote! {
                            let length = #ty::default().aligned_size();
                            let mut v = rkyv::util::AlignedVec::<4>::new();
                            v.extend_from_slice(&bytes[offset..offset + length]);
                            let archived = unsafe { rkyv::access_unchecked::<<#ty as Archive>::Archived>(&v[..]) };
                            let #ident = rkyv::deserialize::<_, rkyv::rancor::Error>(archived).expect("data should be valid");
                            offset += length;
                        }
                    }
                })
                .collect();
            let fields: Vec<_> = self
                .struct_def
                .fields
                .iter()
                .map(|f| f.ident.clone().unwrap())
                .collect();

            Ok(quote! {
                fn from_bytes(bytes: &[u8]) -> Self {
                    let size_length = #size_type::default().aligned_size();
                    let archived =
                        unsafe { rkyv::access_unchecked::<<#size_type as Archive>::Archived>(&bytes[0..size_length]) };
                    let size =
                        rkyv::deserialize::<#size_type, rkyv::rancor::Error>(archived).expect("data should be valid");
                    let mut offset = size_length;

                    #(#field_deserialize)*

                    Self {
                        #(#fields),*
                    }
                }
            })
        } else {
            todo!("Add named size's search");
        }
    }
}
