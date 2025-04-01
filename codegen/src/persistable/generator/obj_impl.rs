use proc_macro2::{Ident, Span, TokenStream};

use crate::persistable::generator::persistable_impl::is_primitive;
use crate::persistable::generator::Generator;

use quote::{quote, ToTokens};
use syn::GenericParam;

impl Generator {
    pub fn gen_obj_impl_def(&self) -> TokenStream {
        if self.is_full_row {
            quote! {}
        } else {
            let struct_ident = &self.struct_def.ident;
            let generics = &self.struct_def.generics;
            let trait_bounds: Vec<_> = generics
                .params
                .iter()
                .map(|generic| {
                    if let GenericParam::Type(param) = generic {
                        let ident = &param.ident;
                        quote! {
                            #ident: Default + SizeMeasurable,
                        }
                    } else {
                        quote! {}
                    }
                })
                .collect();
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

            let field_sizes = self.gen_field_sizes_fns();
            let obj_size = self.gen_full_obj_size_fn();

            quote! {
                impl #impl_generics #struct_ident #ty_generics
                #where_clause
                {
                    #field_sizes
                    #obj_size
                }
            }
        }
    }

    pub fn gen_full_obj_size_fn(&self) -> TokenStream {
        let contains_vec = self
            .struct_def
            .fields
            .iter()
            .any(|f| f.ty.to_token_stream().to_string().contains("Vec"));
        if contains_vec {
            let sizes: Vec<_> = self
                .struct_def
                .fields
                .iter()
                .map(|f| {
                    let fn_ident = Ident::new(
                        format!("{}_size", f.ident.clone().unwrap()).as_str(),
                        Span::call_site(),
                    );

                    if f.ty.to_token_stream().to_string().contains("Vec") {
                        quote! {
                            Self::#fn_ident(length)
                        }
                    } else {
                        quote! {
                            Self::#fn_ident()
                        }
                    }
                })
                .collect();
            quote! {
                pub fn persisted_size(length: usize) -> usize {
                    #(#sizes)+*
                }
            }
        } else {
            let sizes: Vec<_> = self
                .struct_def
                .fields
                .iter()
                .map(|f| {
                    let fn_ident = Ident::new(
                        format!("{}_size", f.ident.clone().unwrap()).as_str(),
                        Span::call_site(),
                    );

                    quote! {
                        Self::#fn_ident()
                    }
                })
                .collect();
            quote! {
                pub fn persisted_size() -> usize {
                    #(#sizes)+*
                }
            }
        }
    }

    fn gen_field_sizes_fns(&self) -> TokenStream {
        let field_sizes = self.struct_def.fields.iter().map(|f| {
            if f.ty.to_token_stream().to_string().contains("Vec") {
                let ty = &f.ty;
                let inner_ty_str = ty.to_token_stream().to_string().replace(" ", "");
                let mut inner_ty_str = inner_ty_str.replace("Vec<", "");
                inner_ty_str.pop();
                let inner_ty: TokenStream = inner_ty_str.parse().unwrap();
                let value_fn_ident = Ident::new(
                    format!("{}_value_size", f.ident.clone().unwrap()).as_str(),
                    Span::call_site(),
                );
                let fn_ident = Ident::new(
                    format!("{}_size", f.ident.clone().unwrap()).as_str(),
                    Span::call_site(),
                );
                let len_in_vec = if is_primitive(&inner_ty_str) {
                    quote! {
                        align(length * <#inner_ty as Default>::default().aligned_size()) + 8
                    }
                } else {
                    quote! {
                         length * align8(<#inner_ty as Default>::default().aligned_size()) + 8
                    }
                };
                let len_value = if is_primitive(&inner_ty_str) {
                    quote! {
                        <#inner_ty as Default>::default().aligned_size()
                    }
                } else {
                    quote! {
                        align8(<#inner_ty as Default>::default().aligned_size())
                    }
                };
                quote! {
                    pub fn #value_fn_ident() -> usize {
                         #len_value
                    }
                    pub fn #fn_ident(length: usize) -> usize {
                        #len_in_vec
                    }
                }
            } else {
                let ident = &f.ty;
                let fn_ident = Ident::new(
                    format!("{}_size", f.ident.clone().unwrap()).as_str(),
                    Span::call_site(),
                );
                quote! {
                    pub fn #fn_ident() -> usize {
                        <#ident as Default>::default().aligned_size()
                    }
                }
            }
        });

        quote! {
            #(#field_sizes)*
        }
    }
}
