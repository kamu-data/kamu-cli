// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use crate::{SyncError, SyncListener, SyncOptions, SyncResult};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Pushes one local dataset to a remote destination. In a short transaction
/// it authorizes reading the dataset for the acting principal and resolves
/// the source and the destination; it transfers with no transaction open, and
/// the destination commits by a compare-and-swap of its head where it
/// supports one. `kamu push` runs it
#[async_trait::async_trait]
pub trait SyncToJob: Send + Sync {
    async fn run(
        &self,
        src: &odf::DatasetHandle,
        dst: odf::DatasetRefRemote,
        options: SyncOptions,
        maybe_listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
