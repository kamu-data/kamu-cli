// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{Expr, Ident, LitStr, Path, Token, parse_macro_input, parse_str};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[proc_macro]
pub fn kamu_cli_run_api_server_e2e_test(input: TokenStream) -> TokenStream {
    let harness_method = parse_str("run_api_server").unwrap();

    kamu_cli_e2e_test_impl(&harness_method, input)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[proc_macro]
pub fn kamu_cli_execute_command_e2e_test(input: TokenStream) -> TokenStream {
    let harness_method = parse_str("execute_command").unwrap();

    kamu_cli_e2e_test_impl(&harness_method, input)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Generates a pair of resource-CLI e2e tests from a single scenario fn that
/// takes a `ResourceCtx`. From one invocation per storage it emits two
/// separately-runnable tests:
///
/// - `<fixture>_local` — runs the scenario against the implicit `local` context
///   (via the `execute_command` harness; the scenario gets
///   `ResourceCtx::Local`).
/// - `<fixture>_remote` — boots an API server and runs the scenario against a
///   remote context (via `run_api_server`; the scenario gets a `ResourceCtx`
///   built by `ResourceCtx::remote_from_server`). Remote is always
///   multi-tenant.
///
/// This removes the hand-written `_local`/`_remote` wrapper fns and collapses
/// the two per-DB macro invocations into one. Author scenarios as a single
/// `pub async fn test_xxx(ctx: ResourceCtx)`.
///
/// Params mirror the other macros: `storage` (required), `fixture` (required),
/// `options` (optional; applied to both arms, remote also gets
/// `with_multi_tenant`), `extra_test_groups` (optional).
#[proc_macro]
pub fn kamu_cli_resource_e2e_test(input: TokenStream) -> TokenStream {
    let InputArgs {
        storage,
        fixture,
        options,
        extra_test_groups,
    } = parse_macro_input!(input as InputArgs);

    let base_name = fixture.segments.last().unwrap().ident.clone();
    let local_name = format_ident!("{base_name}_local");
    let remote_name = format_ident!("{base_name}_remote");

    let base_options = options.unwrap_or_else(|| parse_str("Options::default()").unwrap());
    // Remote requires multi-tenant for `login_as_e2e_user`.
    let remote_options = quote! { (#base_options).with_multi_tenant() };

    let extra_test_groups = if let Some(extra_test_groups) = extra_test_groups {
        parse_str(extra_test_groups.value().as_str()).unwrap()
    } else {
        quote! {}
    };

    // The scenario builds `ResourceCtx`; reference it by its canonical path so
    // the wiring file does not need an extra `use`.
    let ctx_path = quote! { ::kamu_cli_e2e_repo_tests::resources::ResourceCtx };

    let local_body = quote! {
        |kamu| async move {
            #fixture( #ctx_path ::Local(kamu)).await;
        }
    };
    let remote_body = quote! {
        |mut client| async move {
            let ctx = #ctx_path ::remote_from_server(
                &mut client,
                #ctx_path ::DEFAULT_REMOTE_CONTEXT,
            )
            .await;
            #fixture(ctx).await;
        }
    };

    let output = match storage.to_string().as_str() {
        "postgres" => quote! {
            #[test_group::group(e2e, database, postgres, #extra_test_groups)]
            #[test_log::test(sqlx::test(migrator = "database_common::POSTGRES_MIGRATOR"))]
            async fn #local_name (pg_pool: sqlx::PgPool) {
                KamuCliApiServerHarness::postgres(&pg_pool, #base_options )
                    .execute_command( #local_body )
                    .await;
            }

            #[test_group::group(e2e, database, postgres, #extra_test_groups)]
            #[test_log::test(sqlx::test(migrator = "database_common::POSTGRES_MIGRATOR"))]
            async fn #remote_name (pg_pool: sqlx::PgPool) {
                KamuCliApiServerHarness::postgres(&pg_pool, #remote_options )
                    .run_api_server( #remote_body )
                    .await;
            }
        },
        "mysql" => quote! {
            #[test_group::group(e2e, database, mysql, #extra_test_groups)]
            #[test_log::test(sqlx::test(migrator = "database_common::MYSQL_MIGRATOR"))]
            async fn #local_name (mysql_pool: sqlx::MySqlPool) {
                KamuCliApiServerHarness::mysql(&mysql_pool, #base_options )
                    .execute_command( #local_body )
                    .await;
            }

            #[test_group::group(e2e, database, mysql, #extra_test_groups)]
            #[test_log::test(sqlx::test(migrator = "database_common::MYSQL_MIGRATOR"))]
            async fn #remote_name (mysql_pool: sqlx::MySqlPool) {
                KamuCliApiServerHarness::mysql(&mysql_pool, #remote_options )
                    .run_api_server( #remote_body )
                    .await;
            }
        },
        "sqlite" => quote! {
            // kamu-cli will create sqlite database by itself and apply migrations to it
            #[test_group::group(e2e, #extra_test_groups)]
            #[test_log::test(tokio::test)]
            async fn #local_name () {
                KamuCliApiServerHarness::sqlite( #base_options )
                    .execute_command( #local_body )
                    .await;
            }

            #[test_group::group(e2e, #extra_test_groups)]
            #[test_log::test(tokio::test)]
            async fn #remote_name () {
                KamuCliApiServerHarness::sqlite( #remote_options )
                    .run_api_server( #remote_body )
                    .await;
            }
        },
        unexpected => {
            panic!(
                "Unexpected E2E test storage: \"{unexpected}\"!\nAllowable values: \"postgres\", \
                 \"mysql\", and \"sqlite\"."
            );
        }
    };

    output.into()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Implementations
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn kamu_cli_e2e_test_impl(harness_method: &Ident, input: TokenStream) -> TokenStream {
    let InputArgs {
        storage,
        fixture,
        options,
        extra_test_groups,
    } = parse_macro_input!(input as InputArgs);

    let test_function_name = fixture.segments.last().unwrap().ident.clone();

    let options = options.unwrap_or_else(|| parse_str("Options::default()").unwrap());

    let extra_test_groups = if let Some(extra_test_groups) = extra_test_groups {
        parse_str(extra_test_groups.value().as_str()).unwrap()
    } else {
        quote! {}
    };

    let output = match storage.to_string().as_str() {
        "postgres" => quote! {
            #[test_group::group(e2e, database, postgres, #extra_test_groups)]
            #[test_log::test(sqlx::test(migrator = "database_common::POSTGRES_MIGRATOR"))]
            async fn #test_function_name (pg_pool: sqlx::PgPool) {
                KamuCliApiServerHarness::postgres(&pg_pool, #options )
                    . #harness_method ( #fixture )
                    .await;
            }
        },
        "mysql" => quote! {
            #[test_group::group(e2e, database, mysql, #extra_test_groups)]
            #[test_log::test(sqlx::test(migrator = "database_common::MYSQL_MIGRATOR"))]
            async fn #test_function_name (mysql_pool: sqlx::MySqlPool) {
                KamuCliApiServerHarness::mysql(&mysql_pool, #options )
                    . #harness_method ( #fixture )
                    .await;
            }
        },
        "sqlite" => quote! {
           // kamu-cli will create sqlite database by itself and apply migrations to it
           #[test_group::group(e2e, #extra_test_groups)]
           #[test_log::test(tokio::test)]
           async fn #test_function_name () {
               KamuCliApiServerHarness::sqlite ( #options )
                   . #harness_method ( #fixture )
                   .await;
           }
        },
        unexpected => {
            panic!(
                "Unexpected E2E test storage: \"{unexpected}\"!\nAllowable values: \"postgres\", \
                 \"mysql\", and \"sqlite\"."
            );
        }
    };

    output.into()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InputArgs {
    pub storage: Ident,
    pub fixture: Path,
    pub options: Option<Expr>,
    pub extra_test_groups: Option<LitStr>,
}

impl Parse for InputArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut storage = None;
        let mut fixture = None;
        let mut options = None;
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
                "options" => {
                    let value: Expr = input.parse()?;

                    options = Some(value);
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

        Ok(InputArgs {
            storage,
            fixture,
            options,
            extra_test_groups,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
