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
use syn::{parse_macro_input, parse_str, Expr, Ident, LitStr, Path, Token};

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
        "inmem" => quote! {
           #[test_group::group(e2e, #extra_test_groups)]
           #[test_log::test(tokio::test)]
           async fn #test_function_name () {
               KamuCliApiServerHarness::inmem ( #options )
                   . #harness_method ( #fixture )
                   .await;
           }
        },
        "postgres" => quote! {
            #[test_group::group(e2e, database, postgres, #extra_test_groups)]
            #[test_log::test(sqlx::test(migrations = "../../../../../migrations/postgres"))]
            async fn #test_function_name (pg_pool: sqlx::PgPool) {
                KamuCliApiServerHarness::postgres(&pg_pool, #options )
                    . #harness_method ( #fixture )
                    .await;
            }
        },
        "mysql" => quote! {
            #[test_group::group(e2e, database, mysql, #extra_test_groups)]
            #[test_log::test(sqlx::test(migrations = "../../../../../migrations/mysql"))]
            async fn #test_function_name (mysql_pool: sqlx::MySqlPool) {
                KamuCliApiServerHarness::mysql(&mysql_pool, #options )
                    . #harness_method ( #fixture )
                    .await;
            }
        },
        "sqlite" => quote! {
            #[test_group::group(e2e, database, sqlite, #extra_test_groups)]
            #[test_log::test(sqlx::test(migrations = "../../../../../migrations/sqlite"))]
            async fn #test_function_name (sqlite_pool: sqlx::SqlitePool) {
                KamuCliApiServerHarness::sqlite(&sqlite_pool, #options )
                    . #harness_method ( #fixture )
                    .await;
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

        Ok(InputArgs {
            storage,
            fixture,
            options,
            extra_test_groups,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
