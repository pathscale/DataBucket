use proc_macro2::{Ident, Literal, TokenStream};
use quote::__private::Span;
use quote::{quote, ToTokens};
use syn::ItemStruct;

use std::collections::HashMap;

pub struct Generator {
    struct_def: ItemStruct,
    field_types: HashMap<Ident, TokenStream>,
}

impl Generator {
    pub fn new(struct_def: ItemStruct) -> Self {
        Self {
            struct_def,
            field_types: HashMap::new(),
        }
    }

    pub fn gen_persist_type(&mut self) -> syn::Result<TokenStream> {
        let name_ident = Ident::new(
            format!("{}Persisted", self.struct_def.ident).as_str(),
            Span::mixed_site(),
        );
        let mut fields = vec![];
        let mut types = vec![];

        for field in &self.struct_def.fields {
            fields.push(field.ident.clone().unwrap());
            let index_type = field.ty.to_token_stream().to_string();
            let mut split = index_type.split("<");
            // skip `TreeIndex`
            split.next();
            let substr = split.next().unwrap().to_string();
            types.push(substr.split(",").next().unwrap().to_string());
        }

        let fields: Vec<_> = fields
            .into_iter()
            .zip(types)
            .map(|(i, t)| {
                let t: TokenStream = t.parse().unwrap();
                self.field_types.insert(i.clone(), t.clone());
                quote! {
                    #i: Vec<GeneralPage<IndexData<#t>>>,
                }
            })
            .collect();

        Ok(quote! {
            #[derive(Debug, Default, Clone)]
            pub struct #name_ident {
                #(#fields)*
            }
        })
    }

    pub fn gen_persist_impl(&mut self) -> syn::Result<TokenStream> {
        let name_ident = Ident::new(
            format!("{}Persisted", self.struct_def.ident).as_str(),
            Span::mixed_site(),
        );
        let field_names_list: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .map(|f| (Literal::string(f.ident.as_ref().unwrap().to_string().as_str()), f.ident.as_ref().unwrap()))
            .map(|(l, i)| {
                quote! {
                    let i = Interval (
                        self.#i.first().unwrap().header.page_id.into(),
                        self.#i.last().unwrap().header.page_id.into()
                    );
                    map.insert(#l.to_string(), vec![i]);
                }
            })
            .collect();
        let last_header: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .map(|f| f.ident.as_ref().unwrap())
            .map(|i| {
                quote! {
                    if header.is_none() {
                        header = Some(&mut self.#i.last_mut().unwrap().header);
                    } else {
                        let new_header = &mut self.#i.last_mut().unwrap().header;
                        if header.as_ref().unwrap().page_id < new_header.page_id {
                            header = Some(new_header)
                        }
                    }
                }
            })
            .collect();
        let persist_logic = self
            .struct_def
            .fields
            .iter()
            .map(|f| f.ident.as_ref().unwrap())
            .map(|i| {
                quote! {
                    for page in &self.#i {
                        persist_page(&page, file)?;
                    }
                }
            })
            .collect::<Vec<_>>();

        Ok(quote! {
            impl #name_ident {
                pub fn get_intervals(&self) -> std::collections::HashMap<String, Vec<Interval>> {
                    let mut map = std::collections::HashMap::new();

                    #(#field_names_list)*

                    map
                }

                pub fn get_last_header_mut(&mut self) -> &mut GeneralHeader {
                    let mut header = None;

                    #(#last_header)*

                    header.unwrap()
                }

                pub fn persist(&self, file: &mut std::fs::File) -> eyre::Result<()> {
                    #(#persist_logic)*

                    Ok(())
                }
            }
        })
    }

    pub fn gen_persistable_impl(&self) -> syn::Result<TokenStream> {
        let ident = &self.struct_def.ident;
        let name_ident = Ident::new(
            format!("{}Persisted", self.struct_def.ident).as_str(),
            Span::mixed_site(),
        );
        let name = self.struct_def.ident.to_string().replace("Index", "");
        let const_name = Ident::new(
            format!("{}_PAGE_SIZE", name.to_uppercase()).as_str(),
            Span::mixed_site(),
        );

        let field_names_lits: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .map(|f| Literal::string(f.ident.as_ref().unwrap().to_string().as_str()))
            .map(|l| quote! { #l, })
            .collect();
        let idents = self
            .struct_def
            .fields
            .iter()
            .map(|f| f.ident.as_ref().unwrap())
            .collect::<Vec<_>>();
        let field_names_init: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .map(|f| {
                (
                    Literal::string(f.ident.as_ref().unwrap().to_string().as_str()),
                    f.ident.as_ref().unwrap(),
                    !f.ty
                        .to_token_stream()
                        .to_string()
                        .to_lowercase()
                        .contains("lockfree"),
                )
            })
            .map(|(l, i, is_unique)| {
                let ty = self.field_types.get(&i).unwrap();
                if is_unique {
                    quote! {
                        let mut #i = map_index_pages_to_general(map_unique_tree_index::<#ty, #const_name>(&self.#i), previous_header);
                        previous_header = &mut #i.last_mut().unwrap().header;
                    }
                } else {
                    quote! {
                        let mut #i =  map_index_pages_to_general(map_tree_index::<#ty, #const_name>(&self.#i), previous_header);
                        previous_header = &mut #i.last_mut().unwrap().header;
                    }
                }
            })
            .collect();

        Ok(quote! {
            impl PersistableIndex for #ident {
                type PersistedIndex = #name_ident;

                fn get_index_names(&self) -> Vec<&str> {
                    vec![#(#field_names_lits)*]
                }

                fn get_persisted_index(&self, header: &mut GeneralHeader) -> Self::PersistedIndex {
                    let mut previous_header = header;

                    #(#field_names_init)*

                    Self::PersistedIndex {
                        #(#idents,)*
                    }
                }
            }
        })
    }
}
