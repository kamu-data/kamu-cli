// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_resources_facade::ResourceFacade;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Identifies the two fixture accounts used by the contract suite.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::AsRefStr, strum::EnumIter)]
pub enum TestAccount {
    #[strum(serialize = "alice")]
    Alice,
    #[strum(serialize = "bob")]
    Bob,
}

impl TestAccount {
    pub fn name(&self) -> &str {
        self.as_ref()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Common interface that both `LocalFacadeHarness` and
/// `RemoteGraphqlFacadeHarness` implement so that every contract test function
/// is generic over the concrete harness.
#[async_trait::async_trait]
pub trait FacadeContractHarness: Send + Sync {
    /// Returns a `ResourceFacade` scoped to the given account.
    fn facade_for(&self, account: TestAccount) -> Arc<dyn ResourceFacade>;

    /// Returns a *local* `ResourceFacade` scoped to the given account.
    ///
    /// For the local harness this is identical to `facade_for`.  For the
    /// remote harness it returns the underlying `LocalResourceFacadeImpl` that
    /// shares the same in-memory store as the remote facade, enabling
    /// cross-facade tests that write through one transport and read through
    /// the other.
    fn local_facade_for(&self, account: TestAccount) -> Arc<dyn ResourceFacade> {
        self.facade_for(account)
    }

    /// Returns the stable `AccountID` for the given fixture account.
    fn account_id(&self, account: TestAccount) -> odf::AccountID;

    /// Returns the stable `AccountName` for the given fixture account.
    fn account_name(&self, account: TestAccount) -> odf::AccountName;

    /// Flush all pending outbox messages (no-op for Immediate mode).
    ///
    /// Only meaningful when the harness was constructed with
    /// `OutboxProvider::Dispatching`.  Tests that need to observe intermediate
    /// resource phases (e.g. RF-112) must call this after writes and before
    /// reading phase-sensitive state.
    async fn flush_outbox(&self) {}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
