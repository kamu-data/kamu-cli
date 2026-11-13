// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A dataset a pull created in storage only: its `Seed` is written, it has
/// no `HEAD` and no dataset entry until the transfer is committed
pub(super) struct StoredSeed {
    pub dataset: Arc<dyn odf::Dataset>,
    pub seed_block: odf::MetadataBlockTyped<odf::metadata::Seed>,
    pub seed_block_bytes: odf::MetadataBlockBytes,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
