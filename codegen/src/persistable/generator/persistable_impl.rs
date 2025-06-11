use crate::persistable::generator::Generator;

use proc_macro2::{Ident, Span, TokenStream};
use quote::{quote, ToTokens};
use syn::spanned::Spanned;
use syn::{Field, GenericParam, Type};

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
                let mut bytes = vec![];
                #(#field_serialize)*
                bytes
            }
        }
    }

    fn gen_perisistable_by_parts_from_bytes_fn(&self) -> syn::Result<TokenStream> {
        let size_fields: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .enumerate()
            .filter(|(_, f)| f.ident.clone().unwrap().to_string().contains("size"))
            .collect();
        let gens: Vec<_> = self
            .struct_def
            .generics
            .params
            .iter()
            .filter_map(|p| {
                if let GenericParam::Type(t) = p {
                    Some(t.ident.to_string())
                } else {
                    None
                }
            })
            .collect();

        if size_fields.len() == 1 {
            let (pos, size_field) = size_fields.first().unwrap();
            if size_field.ident.as_ref().unwrap() != "size" {
                return Err(syn::Error::new(
                    size_field.span(),
                    "If single size is defined, it should have name `size`",
                ));
            }
            if *pos != 0 {
                return Err(syn::Error::new(
                    size_field.span(),
                    "`size` field should be first field in a struct",
                ));
            }
        } else {
            let mut correct_order = true;
            for i in 0..size_fields.len() {
                correct_order = size_fields.iter().any(|(pos, _)| *pos == i)
            }
            if !correct_order {
                return Err(syn::Error::new(
                    self.struct_def.span(),
                    "`size_..` fields should be first fields in a struct",
                ));
            }
        }

        let field_deserialize: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .filter(|f| !f.ident.clone().unwrap().to_string().contains("size"))
            .map(|f| {
                let ident = &f.ident.clone().expect("is not tuple struct");
                let field_type_str = f.ty.to_token_stream().to_string();
                if field_type_str.contains("Vec") {
                    self.gen_from_bytes_for_vec(&f.ty, ident, &size_fields)
                } else if field_type_str.contains("String") {
                    self.gen_from_bytes_for_string(&f.ty, ident, &size_fields)
                } else if field_type_str
                    .split("<")
                    .any(|v| gens.contains(&v.replace(">", "").trim().to_string()))
                    && self.is_generic_unsized
                {
                    self.gen_from_bytes_for_unsized_generic(&f.ty, ident, &size_fields)
                } else {
                    self.gen_from_bytes_for_primitive(&f.ty, ident)
                }
            })
            .collect();
        let fields: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .map(|f| f.ident.clone().unwrap())
            .collect();

        let size_defs: Vec<_> = size_fields.into_iter().map(|(_,f)| {
            let size_type = &f.ty;
            let size_ident = f.ident.as_ref().unwrap();
            quote! {
                let size_length = <#size_type as Default>::default().aligned_size();
                let archived =
                    unsafe { rkyv::access_unchecked::<<#size_type as Archive>::Archived>(&bytes[offset..offset + size_length]) };
                let #size_ident =
                    rkyv::deserialize::<#size_type, rkyv::rancor::Error>(archived).expect("data should be valid");
                offset += size_length;
            }
        }).collect();

        Ok(quote! {
            fn from_bytes(bytes: &[u8]) -> Self {
                let mut offset = 0usize;
                #(#size_defs)*

                #(#field_deserialize)*

                Self {
                    #(#fields),*
                }
            }
        })
    }

    fn gen_from_bytes_for_primitive(&self, ty: &Type, ident: &Ident) -> TokenStream {
        quote! {
            let length = <#ty as Default>::default().aligned_size();
            let mut v = rkyv::util::AlignedVec::<4>::new();
            v.extend_from_slice(&bytes[offset..offset + length]);
            let archived = unsafe { rkyv::access_unchecked::<<#ty as Archive>::Archived>(&v[..]) };
            let #ident = rkyv::deserialize::<_, rkyv::rancor::Error>(archived).expect("data should be valid");
            offset += length;
        }
    }

    fn gen_from_bytes_for_vec(
        &self,
        ty: &Type,
        ident: &Ident,
        size_fields: &Vec<(usize, &Field)>,
    ) -> TokenStream {
        let inner_ty_str = ty.to_token_stream().to_string().replace(" ", "");
        let mut inner_ty_str = inner_ty_str.replace("Vec<", "");
        inner_ty_str.pop();
        let inner_ty: TokenStream = inner_ty_str.parse().unwrap();
        let size_ident = if size_fields.len() == 1 {
            size_fields.first().unwrap().1.ident.as_ref().unwrap()
        } else {
            let val = size_fields.iter().find(|(_, f)| {
                f.ident
                    .as_ref()
                    .unwrap()
                    .to_string()
                    .contains(ident.to_string().as_str())
            });
            val.unwrap().1.ident.as_ref().unwrap()
        };
        let value_fn_ident =
            Ident::new(format!("{}_value_size", ident).as_str(), Span::call_site());
        let len = if is_primitive(&inner_ty_str) {
            quote! {
                let values_len = align(#size_ident as usize * <#inner_ty as Default>::default().aligned_size()) + 8;
            }
        } else {
            quote! {
                let values_len = #size_ident as usize * Self::#value_fn_ident() + 8;
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
    }

    fn gen_from_bytes_for_string(
        &self,
        ty: &Type,
        ident: &Ident,
        size_fields: &Vec<(usize, &Field)>,
    ) -> TokenStream {
        let size_ident = if size_fields.len() == 1 {
            size_fields.first().unwrap().1.ident.as_ref().unwrap()
        } else {
            let val = size_fields.iter().find(|(_, f)| {
                f.ident
                    .as_ref()
                    .unwrap()
                    .to_string()
                    .contains(ident.to_string().as_str())
            });
            val.unwrap().1.ident.as_ref().unwrap()
        };
        quote! {
            let values_len = align(#size_ident + 8)
            let mut v = rkyv::util::AlignedVec::<4>::new();
            v.extend_from_slice(&bytes[offset..offset + values_len]);
            let archived =
            unsafe { rkyv::access_unchecked::<<#ty as Archive>::Archived>(&v[..]) };
            let #ident = rkyv::deserialize::<#ty, rkyv::rancor::Error>(archived)
                .expect("data should be valid");
            offset += values_len;
        }
    }

    fn gen_from_bytes_for_unsized_generic(
        &self,
        ty: &Type,
        ident: &Ident,
        size_fields: &Vec<(usize, &Field)>,
    ) -> TokenStream {
        let size_ident = if size_fields.len() == 1 {
            size_fields.first().unwrap().1.ident.as_ref().unwrap()
        } else {
            let val = size_fields.iter().find(|(_, f)| {
                f.ident
                    .as_ref()
                    .unwrap()
                    .to_string()
                    .contains(ident.to_string().as_str())
            });
            val.unwrap().1.ident.as_ref().unwrap()
        };
        quote! {
            let values_len = #size_ident as usize;
            let mut v = rkyv::util::AlignedVec::<4>::new();
            v.extend_from_slice(&bytes[offset..offset + values_len]);
            let archived =
            unsafe { rkyv::access_unchecked::<<#ty as Archive>::Archived>(&v[..]) };
            let #ident = rkyv::deserialize::<#ty, rkyv::rancor::Error>(archived)
                .expect("data should be valid");
            offset += values_len;
        }
    }
}
