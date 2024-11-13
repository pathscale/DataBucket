use proc_macro2::{Ident, Literal, TokenStream};
use quote::__private::Span;
use quote::quote;

use crate::persist_table::generator::Generator;

impl Generator {
    pub fn gen_space_type(&self) -> syn::Result<TokenStream> {
        let name = self.struct_def.ident.to_string().replace("Worktable", "");
        let name_ident = Ident::new(
            format!("{}Space", name).as_str(),
            Span::mixed_site(),
        );
        let index_persisted_ident = Ident::new(
            format!("{}Persisted", self.struct_def.ident).as_str(),
            Span::mixed_site(),
        );

        Ok(quote! {
            #[derive(Debug, Default, Clone)]
            pub struct #name_ident {
                info: GeneralPage<SpaceInfoData>,
                primary_index: Vec<GeneralPage<IndexData>>,
                indexes: #index_persisted_ident,
                data: Vec<GeneralPage<Data>>,
            }
        })
    }

    pub fn gen_space_impls(&self) -> syn::Result<TokenStream> {

        Ok(quote! {

        })
    }

    fn gen_space_info_fn(&self) -> syn::Result<TokenStream> {
        let name = self.struct_def.ident.to_string().replace("Worktable", "");
        let literal_name = Literal::string(name.as_str());

        Ok(quote! {
            pub fn space_info_default(&self) -> GeneralPage<SpaceInfoData> {
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