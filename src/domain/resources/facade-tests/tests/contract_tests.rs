// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod contract;
mod harness;
mod helpers;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Generates `<name>::local` and `<name>::remote` tokio test functions that
/// run the given contract suite function against both harness implementations.
///
/// An optional third argument accepts a `messaging_outbox::OutboxProvider`
/// expression; when supplied the harnesses are constructed with that provider
/// instead of the default `Immediate` mode.  Use this for tests that need to
/// observe intermediate resource phases (e.g. RF-112).
#[macro_export]
macro_rules! contract_test {
    ($mod_name:ident, $suite_fn:path) => {
        mod $mod_name {
            #[test_log::test(tokio::test)]
            async fn local() {
                let h = $crate::harness::LocalFacadeHarness::new().await;
                $suite_fn(&h).await;
            }

            #[test_log::test(tokio::test)]
            async fn remote() {
                let h = $crate::harness::RemoteGraphqlFacadeHarness::new().await;
                $suite_fn(&h).await;
            }
        }
    };

    ($mod_name:ident, $suite_fn:path, $outbox:expr) => {
        mod $mod_name {
            #[test_log::test(tokio::test)]
            async fn local() {
                let opts = $crate::harness::LocalFacadeHarnessOpts {
                    outbox_provider: $outbox,
                };
                let h = $crate::harness::LocalFacadeHarness::new_with_opts(opts).await;
                $suite_fn(&h).await;
            }

            #[test_log::test(tokio::test)]
            async fn remote() {
                let opts = $crate::harness::LocalFacadeHarnessOpts {
                    outbox_provider: $outbox,
                };
                let h = $crate::harness::RemoteGraphqlFacadeHarness::new_with_opts(opts).await;
                $suite_fn(&h).await;
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
