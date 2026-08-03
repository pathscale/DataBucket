use proc_macro2::{Ident, Span, TokenStream};

use crate::persistable::generator::persistable_impl::is_primitive;
use crate::persistable::generator::Generator;

use quote::{quote, ToTokens};
use syn::{Field, GenericParam};

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
                            #ident: DefaultSizeMeasurable,
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
        let size_fields: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .enumerate()
            .filter(|(_, f)| f.ident.clone().unwrap().to_string().contains("size"))
            .map(|(_, f)| {
                let ident = f.ident.as_ref().unwrap();
                quote! {
                    #ident: usize
                }
            })
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

        let sizes: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .map(|f| {
                let fn_ident = Ident::new(
                    format!("{}_size", f.ident.clone().unwrap()).as_str(),
                    Span::call_site(),
                );
                let size_ident = if size_fields.len() == 1 {
                    quote! {
                        size
                    }
                } else {
                    quote! {
                        #fn_ident
                    }
                };

                let field_type_str = f.ty.to_token_stream().to_string();
                if field_type_str.contains("Vec") || field_type_str.contains("String") {
                    quote! {
                        Self::#fn_ident(#size_ident)
                    }
                } else if field_type_str
                    .split("<")
                    .any(|v| gens.contains(&v.replace(">", "").trim().to_string()))
                    && self.is_generic_unsized
                {
                    quote! {
                        #size_ident
                    }
                } else {
                    quote! {
                        Self::#fn_ident()
                    }
                }
            })
            .collect();
        quote! {
                pub fn persisted_size(#(#size_fields),*) -> usize {
                    #(#sizes)+*
                }
        }
    }

    fn gen_field_sizes_fns(&self) -> TokenStream {
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
        let field_sizes = self.struct_def.fields.iter().map(|f| {
            let field_type_str = f.ty.to_token_stream().to_string();
            if field_type_str.contains("Vec") {
                self.gen_vec_size_fns(f)
            } else if field_type_str.contains("String") {
                self.gen_string_size_fn(f)
            } else if field_type_str
                .split("<")
                .any(|v| gens.contains(&v.replace(">", "").trim().to_string()))
                && self.is_generic_unsized
            {
                self.gen_generic_size_fn(f)
            } else {
                self.gen_primitive_size_fn(f)
            }
        });

        quote! {
            #(#field_sizes)*
        }
    }

    fn gen_primitive_size_fn(&self, f: &Field) -> TokenStream {
        let ty = &f.ty;
        let fn_ident = Ident::new(
            format!("{}_size", f.ident.clone().unwrap()).as_str(),
            Span::call_site(),
        );
        quote! {
            pub fn #fn_ident() -> usize {
                <#ty as DefaultSizeMeasurable>::default_aligned_size()
            }
        }
    }

    fn gen_string_size_fn(&self, f: &Field) -> TokenStream {
        let fn_ident = Ident::new(
            format!("{}_size", f.ident.clone().unwrap()).as_str(),
            Span::call_site(),
        );
        quote! {
            pub fn #fn_ident(length: usize) -> usize {
                align(length + 8)
            }
        }
    }

    fn gen_generic_size_fn(&self, f: &Field) -> TokenStream {
        let fn_ident = Ident::new(
            format!("{}_size", f.ident.clone().unwrap()).as_str(),
            Span::call_site(),
        );
        let ty_ = &f.ty;
        quote! {
            pub fn #fn_ident(length: usize) -> usize {
                <#ty_ as VariableSizeMeasurable>::aligned_size(length)
            }
        }
    }

    fn gen_vec_size_fns(&self, f: &Field) -> TokenStream {
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
                align(length * <#inner_ty as DefaultSizeMeasurable>::default_aligned_size()) + 8
            }
        } else {
            quote! {
                 length * Self::#value_fn_ident() + 8
            }
        };
        let len_value = if is_primitive(&inner_ty_str) {
            quote! {
                <#inner_ty as DefaultSizeMeasurable>::default_aligned_size()
            }
        } else {
            quote! {
                if <#inner_ty as SizeMeasurable>::align() == Some(8) {
                    align8(<#inner_ty as DefaultSizeMeasurable>::default_aligned_size())
                } else {
                    <#inner_ty as DefaultSizeMeasurable>::default_aligned_size()
                }
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
    }
}
