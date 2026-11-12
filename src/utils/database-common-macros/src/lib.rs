// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{
    ExprArray,
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
    parse_macro_input,
    parse_quote,
    parse_str,
};

mod transaction_block;

use transaction_block::{Binding, TransactionBlock};

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

    // The transaction catalog shadows the request catalog in the body
    TransactionBlock::wrap_function(
        parse_quote!(catalog),
        vec![Binding::transaction_catalog("catalog")],
        input,
    )
    .unwrap_or_else(syn::Error::into_compile_error)
    .into()
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

#[proc_macro_attribute]
/// Encrusting the method with a transactional Catalog.
/// The structure must contain the Catalog as a field
///
/// # Examples
/// ```compile_fail
/// #[transactional_method()]
/// async fn set_system_flow_schedule(&self) {
///     // `transaction_catalog` is available inside the method body
/// }
/// ```
pub fn transactional_method(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    TransactionBlock::wrap_function(
        parse_quote!(self.catalog),
        vec![Binding::transaction_catalog("transaction_catalog")],
        input,
    )
    .unwrap_or_else(syn::Error::into_compile_error)
    .into()
}

#[proc_macro_attribute]
/// Encrusting the method with a transactional Catalog.
/// The structure must contain the Catalog as a field
///
/// # Examples
/// ```compile_fail
/// // `service` request from a transactional Catalog
/// #[transactional_method1(service: Arc<dyn Service>)]
/// async fn set_system_flow_schedule(&self) {
///     // `service` is available inside the method body
/// }
/// ```
pub fn transactional_method1(attr: TokenStream, item: TokenStream) -> TokenStream {
    transactional_method_with(1, attr, item)
}

#[proc_macro_attribute]
/// Encrusting the method with a transactional Catalog.
/// The structure must contain the Catalog as a field
///
/// # Examples
/// ```compile_fail
/// // `service` request from a transactional Catalog
/// #[transactional_method2(service1: Arc<dyn Service1>, service2: Arc<dyn Service2>)]
/// async fn set_system_flow_schedule(&self) {
///     // `service1` and `service2` are available inside the method body
/// }
/// ```
pub fn transactional_method2(attr: TokenStream, item: TokenStream) -> TokenStream {
    transactional_method_with(2, attr, item)
}

#[proc_macro_attribute]
/// Encrusting the method with a transactional Catalog.
/// The structure must contain the Catalog as a field
///
/// # Examples
/// ```compile_fail
/// // `service` request from a transactional Catalog
/// #[transactional_method3(service1: Arc<dyn Service1>, service2: Arc<dyn Service2>, service3: Arc<dyn Service3>)]
/// async fn set_system_flow_schedule(&self) {
///     // `service1`, `service2`, and `service3` are available inside the method body
/// }
/// ```
pub fn transactional_method3(attr: TokenStream, item: TokenStream) -> TokenStream {
    transactional_method_with(3, attr, item)
}

#[proc_macro_attribute]
/// Encrusting the method with a transactional Catalog.
/// The structure must contain the Catalog as a field
///
/// # Examples
/// ```compile_fail
/// // `service` request from a transactional Catalog
/// #[transactional_method4(service1: Arc<dyn Service1>, service2: Arc<dyn Service2>, service3: Arc<dyn Service3>, service4: Arc<dyn Service4>)]
/// async fn set_system_flow_schedule(&self) {
///     // `service1`, `service2`, `service3`, and `service4` are available inside the method body
/// }
/// ```
pub fn transactional_method4(attr: TokenStream, item: TokenStream) -> TokenStream {
    transactional_method_with(4, attr, item)
}

fn transactional_method_with(arity: usize, attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    Binding::parse_exactly(arity, attr.into())
        .and_then(|bindings| {
            TransactionBlock::wrap_function(parse_quote!(self.catalog), bindings, input)
        })
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// transaction!
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs a block of code in a transaction of the catalog's
/// `DatabaseTransactionManager`: the transaction commits when the block
/// succeeds and rolls back when it fails. Write one block per transaction of an
/// algorithm that runs several in sequence. The block borrows the locals it uses,
/// as straight-line code does.
///
/// The bindings of the closure-shaped body are resolved from the transaction:
/// `name: Arc<dyn Iface>` is a component, and a bare `name` is the transaction
/// catalog.
///
/// Without a return type the block evaluates to the body's value, and `?` in it
/// leaves the enclosing function, whose error type must implement
/// `From<InternalError>`; `return` is refused there. With `-> Result<T, E>` the
/// block evaluates to the body's `Result`, and `return` leaves the body.
///
/// The transaction comes from `self.catalog` unless attributes before the body
/// say otherwise:
/// - `#[catalog(<dill::Catalog>)]` opens it from another catalog;
/// - `#[when(<bool>)]` opens it only when true; otherwise the body runs on the catalog as given;
/// - `#[span(<tracing::Span>)]` replaces the block's span, by default a debug span named
///   `transaction` at the call site.
///
/// # Examples
/// ```compile_fail
/// // 1: Plan
/// let plan = transaction!(|planner: Arc<dyn Planner>| {
///     planner.plan(&request).await?
/// });
///
/// // 2: Run, outside any transaction
/// let response = self.engine.execute(&plan).await?;
///
/// // 3: Commit, handling the error here
/// let committed = transaction!(
///     #[catalog(base_catalog)]
///     |committer: Arc<dyn Committer>| -> Result<_, CommitError> {
///         committer.commit(&response).await
///     }
/// );
/// ```
#[proc_macro]
pub fn transaction(input: TokenStream) -> TokenStream {
    parse_macro_input!(input as TransactionBlock)
        .expand()
        .into()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// #[database_transactional_test]
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[proc_macro]
pub fn database_transactional_test(input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(input as DatabaseTransactionalTestInputArgs);
    let fixture = &args.fixture;
    let run_fixture = quote! { #fixture (&catalog).await; };
    let run_fixture_in_transaction = run_in_transaction(fixture);

    let output = match args.storage.to_string().as_str() {
        "inmem" => args.inmem_test(&args.test_function_name(), &harness_catalog(), &run_fixture),
        // Also runs the fixture inside one transaction of `InMemoryTransactionManager`
        "inmem_with_transaction" => {
            let plain =
                args.inmem_test(&args.test_function_name(), &harness_catalog(), &run_fixture);
            let in_transaction = args.inmem_test(
                &format_ident!("{}_in_transaction", args.test_function_name()),
                &harness_catalog_with_in_memory_transaction_manager(),
                &run_fixture_in_transaction,
            );
            quote! { #plain #in_transaction }
        }
        "postgres" => args.database_test(Database::Postgres, &run_fixture_in_transaction),
        "sqlite" => args.database_test(Database::Sqlite, &run_fixture_in_transaction),
        unexpected => {
            panic!(
                "Unexpected E2E test storage: \"{unexpected}\"!\nAllowable values: \"inmem\", \
                 \"inmem_with_transaction\", \"postgres\", and \"sqlite\"."
            );
        }
    };

    output.into()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// #[database_concurrent_transactions_test]
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Like `database_transactional_test!`, but the fixture runs outside any
/// transaction and gets the harness catalog with its transaction manager, to
/// open several transactions itself. `storage` is `inmem` (which adds
/// `InMemoryTransactionManager` to the harness catalog), `postgres` or `sqlite`.
/// Wire only fixtures whose transactions run one after another, such as a
/// rollback, to `sqlite`: SQLite serialises writers, so a race there reduces to
/// a sequential test.
#[proc_macro]
pub fn database_concurrent_transactions_test(input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(input as DatabaseTransactionalTestInputArgs);
    let fixture = &args.fixture;
    let run_fixture = quote! { #fixture (&catalog).await; };

    let output = match args.storage.to_string().as_str() {
        "inmem" => args.inmem_test(
            &args.test_function_name(),
            &harness_catalog_with_in_memory_transaction_manager(),
            &run_fixture,
        ),
        "postgres" => args.database_test(Database::Postgres, &run_fixture),
        "sqlite" => args.database_test(Database::Sqlite, &run_fixture),
        unexpected => {
            panic!(
                "Unexpected storage: \"{unexpected}\"!\nAllowable values: \"inmem\", \"postgres\" \
                 and \"sqlite\"."
            );
        }
    };

    output.into()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy)]
enum Database {
    Postgres,
    Sqlite,
}

/// Runs the fixture on the catalog of a transaction opened on `catalog`,
/// committing it
fn run_in_transaction(fixture: &Path) -> TokenStream2 {
    quote! {
        database_common::DatabaseTransactionRunner::from(catalog)
            .transactional(|transaction_catalog| async move {
                #fixture (&transaction_catalog).await;

                Ok::<_, internal_error::InternalError>(())
            })
            .await
            .unwrap();
    }
}

/// Binds `catalog` to the harness catalog
fn harness_catalog() -> TokenStream2 {
    quote! {
        let catalog = harness.catalog;
    }
}

/// Binds `catalog` to one chained on the harness catalog with
/// `InMemoryTransactionManager`, so that transactions can be opened on it
fn harness_catalog_with_in_memory_transaction_manager() -> TokenStream2 {
    quote! {
        let catalog = {
            let mut catalog_builder = dill::CatalogBuilder::new_chained(&harness.catalog);
            catalog_builder.add::<database_common::InMemoryTransactionManager>();
            catalog_builder.build()
        };
    }
}

impl DatabaseTransactionalTestInputArgs {
    fn test_function_name(&self) -> Ident {
        self.fixture.segments.last().unwrap().ident.clone()
    }

    fn extra_test_groups(&self) -> TokenStream2 {
        match &self.extra_test_groups {
            Some(extra_test_groups) => parse_str(extra_test_groups.value().as_str()).unwrap(),
            None => quote! {},
        }
    }

    /// A test building the harness with `new()`, binding `catalog` with
    /// `bind_catalog` and running `body`
    fn inmem_test(
        &self,
        name: &Ident,
        bind_catalog: &TokenStream2,
        body: &TokenStream2,
    ) -> TokenStream2 {
        let harness = &self.harness;
        let extra_test_groups = self.extra_test_groups();
        quote! {
            #[test_group::group(#extra_test_groups)]
            #[test_log::test(tokio::test)]
            async fn #name () {
                let harness = #harness ::new();
                #bind_catalog

                #body
            }
        }
    }

    /// A test on a fresh database whose migrations ran, building the harness
    /// with `new(pool)` and running `body` on its catalog as `catalog`
    fn database_test(&self, database: Database, body: &TokenStream2) -> TokenStream2 {
        let name = self.test_function_name();
        let harness = &self.harness;
        let extra_test_groups = self.extra_test_groups();
        let tables_for_cleanup = tables_for_cleanup();
        let (group, migrations, pool_type) = match database {
            Database::Postgres => (
                quote! { database, postgres },
                "../../../../migrations/postgres",
                quote! { sqlx::PgPool },
            ),
            Database::Sqlite => (
                quote! { sqlite },
                "../../../../migrations/sqlite",
                quote! { sqlx::SqlitePool },
            ),
        };
        quote! {
            #[test_group::group(#group, #extra_test_groups)]
            #[test_log::test(sqlx::test(migrations = #migrations))]
            async fn #name (pool: #pool_type) {
                for table in #tables_for_cleanup {
                    sqlx::query(sqlx::AssertSqlSafe(format!("DELETE FROM {table}")))
                    .execute(&pool)
                    .await
                    .unwrap();
                }

                let harness = #harness ::new(pool);
                let catalog = harness.catalog;

                #body
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Tables to clean up before a database test, as migrations add rows to them
fn tables_for_cleanup() -> ExprArray {
    parse_str::<ExprArray>(
        r#"
        [
            "outbox_message_consumptions",
        ]
        "#,
    )
    .unwrap()
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
            }

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
