// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{
    parse_macro_input,
    parse_str,
    FnArg,
    GenericArgument,
    Ident,
    ItemFn,
    LitStr,
    Pat,
    PatTupleStruct,
    Path,
    PathArguments,
    Token,
    Type,
    TypePath,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// #[transactional_handler]
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[proc_macro_attribute]
pub fn transactional_handler(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    let has_catalog_argument = input.sig.inputs.iter().any(is_catalog_argument);

    let function_name = &input.sig.ident;
    if !has_catalog_argument {
        const CATALOG_ARGUMENT: &str = "Extension(catalog): Extension<Catalog>";
        panic!("{function_name}(): the expected argument \"{CATALOG_ARGUMENT}\" was not found!");
    }

    let function_signature = &input.sig;
    let function_body = &input.block;
    let function_visibility = &input.vis;

    let updated_function = quote! {
        #function_visibility #function_signature {
            use tracing::Instrument;
            ::database_common::DatabaseTransactionRunner::new(catalog)
                .transactional(|catalog: ::dill::Catalog| async move {
                    #function_body
                })
                .instrument(tracing::debug_span!(stringify!(#function_name)))
                .await
        }
    };

    TokenStream::from(updated_function)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn is_catalog_argument(argument: &FnArg) -> bool {
    let FnArg::Typed(typed_pattern) = argument else {
        return false;
    };

    let Pat::TupleStruct(tuple_struct_pattern) = typed_pattern.pat.as_ref() else {
        return false;
    };

    if !is_catalog_tuple_struct(tuple_struct_pattern) {
        return false;
    }

    let Type::Path(ref type_path) = *typed_pattern.ty else {
        return false;
    };

    is_catalog_type(type_path)
}

#[test]
fn test_is_catalog_argument_works_correctly() {
    let mut inputs = vec![];

    for tuple_type in ["Extension", "axum::Extension", "::axum::Extension"] {
        for argument_type in ["Extension", "axum::Extension", "::axum::Extension"] {
            for generic_type in ["Catalog", "dill::Catalog", "::dill::Catalog"] {
                let input: FnArg = syn::parse_str(&format!(
                    "{tuple_type}(catalog): {argument_type}<{generic_type}>"
                ))
                .unwrap();

                inputs.push(input);
            }
        }
    }

    for input in inputs {
        assert!(is_catalog_argument(&input));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// ::axum::Extension(catalog): ::axum::Extension<::dill::Catalog>,
//   ^^^^^^^^^^^^^^^^^^^^^^^^
fn is_catalog_tuple_struct(tuple_struct_pattern: &PatTupleStruct) -> bool {
    // ::axum::Extension(catalog): ::axum::Extension<::dill::Catalog>,
    //         ^^^^^^^^^
    let Some(last_type_segment) = tuple_struct_pattern.path.segments.last() else {
        return false;
    };

    if last_type_segment.ident != "Extension" {
        return false;
    }

    // ::axum::Extension(catalog): ::axum::Extension<::dill::Catalog>,
    //                   ^^^^^^^
    let Some(first_tuple_element) = tuple_struct_pattern.elems.first() else {
        return false;
    };

    let Pat::Ident(tuple_ident) = first_tuple_element else {
        return false;
    };

    tuple_ident.ident == "catalog"
}

// ::axum::Extension(catalog): ::axum::Extension<::dill::Catalog>,
//                               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
fn is_catalog_type(type_path: &TypePath) -> bool {
    // ::axum::Extension(catalog): ::axum::Extension<::dill::Catalog>,
    //                                     ^^^^^^^^^
    let Some(last_type_segment) = type_path.path.segments.last() else {
        return false;
    };

    if last_type_segment.ident != "Extension" {
        return false;
    }

    // ::axum::Extension(catalog): ::axum::Extension<::dill::Catalog>,
    //                                                       ^^^^^^^
    let PathArguments::AngleBracketed(ref generic_arguments) = last_type_segment.arguments else {
        return false;
    };

    let Some(last_generic_argument) = generic_arguments.args.last() else {
        return false;
    };

    let GenericArgument::Type(type_generic_argument) = last_generic_argument else {
        return false;
    };

    let Type::Path(generic_type_path) = type_generic_argument else {
        return false;
    };

    let Some(last_type_segment) = generic_type_path.path.segments.last() else {
        return false;
    };

    last_type_segment.ident == "Catalog"
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Transactional methods
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct CatalogItem1 {
    item_name: Ident,
    item_type: Type,
}

impl Parse for CatalogItem1 {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let item_name: Ident = input.parse()?;

        input.parse::<Token![:]>()?;

        let item_type: Type = input.parse()?;

        Ok(Self {
            item_name,
            item_type,
        })
    }
}

struct CatalogItem2 {
    item1_name: Ident,
    item1_type: Type,
    item2_name: Ident,
    item2_type: Type,
}

impl Parse for CatalogItem2 {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let item1_name: Ident = input.parse()?;

        input.parse::<Token![:]>()?;

        let item1_type: Type = input.parse()?;

        input.parse::<Token![,]>()?;

        let item2_name: Ident = input.parse()?;

        input.parse::<Token![:]>()?;

        let item2_type: Type = input.parse()?;

        Ok(Self {
            item1_name,
            item1_type,
            item2_name,
            item2_type,
        })
    }
}

#[proc_macro_attribute]
/// Encrusting the method with a transactional Catalog.
/// The structure must contain the Catalog as a field
///
/// # Examples
/// ```
/// #[transactional_method()]
/// async fn set_system_flow_schedule(&self) {
///     // `transaction_catalog` is available inside the method body
/// }
/// ```
pub fn transactional_method(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    let method_signature = &input.sig;
    let method_name = &method_signature.ident;
    let method_body = &input.block;
    let method_visibility = &input.vis;

    let updated_method = quote! {
        #method_visibility #method_signature {
            use tracing::Instrument;
            ::database_common::DatabaseTransactionRunner::new(self.catalog.clone())
                .transactional(|transaction_catalog: Catalog| async move {
                    #method_body
                })
                .instrument(tracing::debug_span!(stringify!(#method_name)))
                .await
        }
    };

    TokenStream::from(updated_method)
}

#[proc_macro_attribute]
/// Encrusting the method with a transactional Catalog.
/// The structure must contain the Catalog as a field
///
/// # Examples
/// ```
/// // `service` request from a transactional Catalog
/// #[transactional_method1(service: Arc<dyn Service>)]
/// async fn set_system_flow_schedule(&self) {
///     // `service` is available inside the method body
/// }
/// ```
pub fn transactional_method1(attr: TokenStream, item: TokenStream) -> TokenStream {
    let CatalogItem1 {
        item_name: catalog_item_name,
        item_type: catalog_item_type,
    } = parse_macro_input!(attr as CatalogItem1);
    let input = parse_macro_input!(item as ItemFn);

    let method_signature = &input.sig;
    let method_name = &method_signature.ident;
    let method_body = &input.block;
    let method_visibility = &input.vis;

    let updated_method = quote! {
        #method_visibility #method_signature {
            use tracing::Instrument;
            ::database_common::DatabaseTransactionRunner::new(self.catalog.clone())
                .transactional_with(|#catalog_item_name: #catalog_item_type| async move {
                    #method_body
                })
                .instrument(tracing::debug_span!(stringify!(#method_name)))
                .await
        }
    };

    TokenStream::from(updated_method)
}

#[proc_macro_attribute]
/// Encrusting the method with a transactional Catalog.
/// The structure must contain the Catalog as a field
///
/// # Examples
/// ```
/// // `service` request from a transactional Catalog
/// #[transactional_method2(service1: Arc<dyn Service1>, service2: Arc<dyn Service2>)]
/// async fn set_system_flow_schedule(&self) {
///     // `service1` and `service2` are available inside the method body
/// }
/// ```
pub fn transactional_method2(attr: TokenStream, item: TokenStream) -> TokenStream {
    let CatalogItem2 {
        item1_name: catalog_item1_name,
        item1_type: catalog_item1_type,
        item2_name: catalog_item2_name,
        item2_type: catalog_item2_type,
    } = parse_macro_input!(attr as CatalogItem2);
    let input = parse_macro_input!(item as ItemFn);

    let method_signature = &input.sig;
    let method_name = &method_signature.ident;
    let method_body = &input.block;
    let method_visibility = &input.vis;

    let updated_method = quote! {
        #method_visibility #method_signature {
            use tracing::Instrument;
            ::database_common::DatabaseTransactionRunner::new(self.catalog.clone())
                .transactional_with2(|#catalog_item1_name: #catalog_item1_type, #catalog_item2_name: #catalog_item2_type| async move {
                    #method_body
                })
                .instrument(tracing::debug_span!(stringify!(#method_name)))
                .await
        }
    };

    TokenStream::from(updated_method)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// #[database_transactional_test]
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[proc_macro]
pub fn database_transactional_test(input: TokenStream) -> TokenStream {
    let DatabaseTransactionalTestInputArgs {
        storage,
        fixture,
        harness,
        extra_test_groups,
    } = parse_macro_input!(input as DatabaseTransactionalTestInputArgs);

    let test_function_name = fixture.segments.last().unwrap().ident.clone();

    let extra_test_groups = if let Some(extra_test_groups) = extra_test_groups {
        parse_str(extra_test_groups.value().as_str()).unwrap()
    } else {
        quote! {}
    };

    let output = match storage.to_string().as_str() {
        "inmem" => quote! {
            #[test_group::group(#extra_test_groups)]
            #[test_log::test(tokio::test)]
            async fn #test_function_name () {
                let harness = #harness ::new();

                #fixture (&harness.catalog).await;
            }
        },
        "postgres" => quote! {
            #[test_group::group(database, postgres, #extra_test_groups)]
            #[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
            async fn #test_function_name (pg_pool: sqlx::PgPool) {
                let harness = #harness ::new(pg_pool);

                database_common::DatabaseTransactionRunner::new(harness.catalog)
                    .transactional(|catalog| async move {
                        #fixture (&catalog).await;

                        Ok::<_, internal_error::InternalError>(())
                    })
                    .await
                    .unwrap();
            }
        },
        "mysql" => quote! {
            #[test_group::group(database, mysql, #extra_test_groups)]
            #[test_log::test(sqlx::test(migrations = "../../../../migrations/mysql"))]
            async fn #test_function_name (mysql_pool: sqlx::MySqlPool) {
                let harness = #harness ::new(mysql_pool);

                database_common::DatabaseTransactionRunner::new(harness.catalog)
                    .transactional(|catalog| async move {
                        #fixture (&catalog).await;

                        Ok::<_, internal_error::InternalError>(())
                    })
                    .await
                    .unwrap();
            }
        },
        "sqlite" => quote! {
            #[test_group::group(database, sqlite, #extra_test_groups)]
            #[test_log::test(sqlx::test(migrations = "../../../../migrations/sqlite"))]
            async fn #test_function_name (sqlite_pool: sqlx::SqlitePool) {
                let harness = #harness ::new(sqlite_pool);

                database_common::DatabaseTransactionRunner::new(harness.catalog)
                    .transactional(|catalog| async move {
                        #fixture (&catalog).await;

                        Ok::<_, internal_error::InternalError>(())
                    })
                    .await
                    .unwrap();
            }
        },
        unexpected => {
            panic!(
                "Unexpected E2E test storage: \"{unexpected}\"!\nAllowable values: \"inmem\", \
                 \"postgres\", \"mysql\", and \"sqlite\"."
            );
        }
    };

    output.into()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DatabaseTransactionalTestInputArgs {
    pub storage: Ident,
    pub fixture: Path,
    pub harness: Ident,
    pub extra_test_groups: Option<LitStr>,
}

impl Parse for DatabaseTransactionalTestInputArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut storage = None;
        let mut fixture = None;
        let mut harness = None;
        let mut extra_test_groups = None;

        while !input.is_empty() {
            let key: Ident = input.parse()?;

            input.parse::<Token![=]>()?;

            match key.to_string().as_str() {
                "storage" => {
                    let value: Ident = input.parse()?;

                    storage = Some(value);
                }
                "fixture" => {
                    let value: Path = input.parse()?;

                    fixture = Some(value);
                }
                "harness" => {
                    let value: Ident = input.parse()?;

                    harness = Some(value);
                }
                "extra_test_groups" => {
                    let value: LitStr = input.parse()?;

                    extra_test_groups = Some(value);
                }
                unexpected_key => panic!(
                    "Unexpected key: {unexpected_key}\nAllowable values: \"storage\", \
                     \"fixture\", \"options\", and \"extra_test_groups\"."
                ),
            };

            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }

        let Some(storage) = storage else {
            panic!("Mandatory parameter \"storage\" not found");
        };

        let Some(fixture) = fixture else {
            panic!("Mandatory parameter \"fixture\" not found");
        };

        let Some(harness) = harness else {
            panic!("Mandatory parameter \"harness\" not found");
        };

        Ok(DatabaseTransactionalTestInputArgs {
            storage,
            fixture,
            harness,
            extra_test_groups,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
