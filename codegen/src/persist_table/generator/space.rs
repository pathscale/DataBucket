use proc_macro2::{Ident, Literal, TokenStream};
use quote::__private::Span;
use quote::quote;

use crate::persist_table::generator::Generator;

impl Generator {
    pub fn gen_space_type(&self) -> syn::Result<TokenStream> {
        let name = self.struct_def.ident.to_string().replace("WorkTable", "");
        let pk_type = &self.pk_ident;
        let name_ident = Ident::new(format!("{}Space", name).as_str(), Span::mixed_site());
        let index_persisted_ident = Ident::new(
            format!("{}IndexPersisted", name).as_str(),
            Span::mixed_site(),
        );

        Ok(quote! {
            #[derive(Debug, Clone)]
            pub struct #name_ident {
                pub info: GeneralPage<SpaceInfoData>,
                pub primary_index: Vec<GeneralPage<IndexData<#pk_type>>>,
                pub indexes: #index_persisted_ident,
                //pub data: Vec<GeneralPage<Data>>,
            }
        })
    }

    pub fn gen_space_impls(&self) -> syn::Result<TokenStream> {
        let ident = &self.struct_def.ident;
        let space_info_fn = self.gen_space_info_fn()?;
        let persisted_pk_fn = self.gen_persisted_primary_key_fn()?;
        let into_space = self.gen_into_space()?;

        Ok(quote! {
            impl #ident {
                #space_info_fn
                #persisted_pk_fn
                #into_space
            }
        })
    }

    fn gen_space_info_fn(&self) -> syn::Result<TokenStream> {
        let name = self.struct_def.ident.to_string().replace("WorkTable", "");
        let literal_name = Literal::string(name.as_str());

        Ok(quote! {
            pub fn space_info_default() -> GeneralPage<SpaceInfoData> {
                let inner = SpaceInfoData {
                    id: 0.into(),
                    page_count: 0,
                    name: #literal_name.to_string(),
                    primary_key_intervals: vec![]
                };
                let header = GeneralHeader {
                    page_id: 0.into(),
                    previous_id: 0.into(),
                    next_id: 0.into(),
                    page_type: PageType::SpaceInfo,
                    space_id: 0.into(),
                };
                GeneralPage {
                    header,
                    inner
                }
            }
        })
    }

    fn gen_persisted_primary_key_fn(&self) -> syn::Result<TokenStream> {
        let name = self.struct_def.ident.to_string().replace("WorkTable", "");
        let const_name = Ident::new(
            format!("{}_PAGE_SIZE", name.to_uppercase()).as_str(),
            Span::mixed_site(),
        );
        let pk_type = &self.pk_ident;

        Ok(quote! {
            pub fn get_peristed_primary_key(&self) -> Vec<IndexData<#pk_type>> {
                map_unique_tree_index::<_, #const_name>(&self.0.pk_map)
            }
        })
    }

    fn gen_into_space(&self) -> syn::Result<TokenStream> {
        let ident = &self.index_ident;
        let name = self.struct_def.ident.to_string().replace("WorkTable", "");
        let space_ident = Ident::new(format!("{}Space", name).as_str(), Span::mixed_site());

        Ok(quote! {
            pub fn into_space(&self) -> #space_ident {
                let info = #ident::space_info_default();
                let header = info.header.clone();

                let primary_index = map_index_pages_to_general(self.get_peristed_primary_key(), &mut header);
                let previous_header = &mut primary_index.last_mut().unwrap().header;
                let indexes = self.0.indexes.get_persisted_index(previous_header);

                #space_ident {
                    info,
                    primary_index,
                    indexes,
                }
            }
        })
    }
}

// worktable! (
//     name: Test,
//     persistence: true,
//     columns: {
//         id: Uuid primary_key,
//         another: i64,
//     },
//     indexes: {
//         another_idx: another,
//     }
// );
//
// // Persisted index object. It's generated because Index type itself is generated.
// struct TestIndexPersisted {
//     primary: Vec<GeneralPage<IndexPage<Uuid>>>,
//     another_idx: Vec<GeneralPage<IndexPage<i64>>>,
// }
//
// // Describes file pages structure.
// struct TestSpace {
//     table_info_page: TableInfoPage,
//     indexes: TestIndexPersisted,
//     data: Vec<GeneralPage<DataPage>>,
//
//     persistence_engine: Arc<PersistenceEngine>,
// }
//
// fn test_persist () {
//     let persistence_config = PersistenceEngineConfig {
//         path: "tests/db",
//     };
//     let engine = Arc::new(PersistenceEngine::new(persistence_config));
//
//     let table = TestWorkTable::new(engine.clone());
//
//     let space: TestSpace = table.into_space();
//     // this call will save space file to `tests/db`. It will be `tests/db/test.wt`
//     space.persist();
// }
//
// fn test_read () {
//     let persistence_config = PersistenceEngineConfig {
//         path: "tests/db",
//     };
//     let engine = Arc::new(PersistenceEngine::new(persistence_config));
//
//     let space = TestSpace::read(engine);
//     let table = space.into_table();
// }
