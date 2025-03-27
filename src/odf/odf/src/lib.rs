// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[cfg(feature = "testing")]
pub use odf_dataset::MockDatasetStorageUnitWriter;
pub use odf_dataset::{
    BlockRef,
    Dataset,
    DatasetNotFoundError,
    DatasetRefUnresolvedError,
    DatasetStorageUnit,
    DatasetStorageUnitWriter,
    DatasetVisibility,
    IterBlocksError,
    MetadataChain,
};
pub use odf_metadata::{
    AccessError,
    AccountID,
    AccountName,
    Checkpoint,
    DataSlice,
    DatasetAlias,
    DatasetAliasRemote,
    DatasetHandle,
    DatasetID,
    DatasetKind,
    DatasetName,
    DatasetPushTarget,
    DatasetRef,
    DatasetRefAny,
    DatasetRefAnyPattern,
    DatasetRefPattern,
    DatasetRefRemote,
    DatasetSnapshot,
    MetadataBlock,
    MetadataBlockTyped,
    MetadataEvent,
    Multihash,
    RepoName,
};
pub use odf_storage::{GetBlockError, GetRefError};

pub mod metadata {
    pub use odf_metadata::*;
}

pub mod dataset {
    pub use odf_dataset::*;
    pub use odf_dataset_impl::*;
}

pub mod storage {
    pub use odf_storage::*;
    #[cfg(feature = "http")]
    pub use odf_storage_http as http;
    pub use odf_storage_inmem as inmem;
    #[cfg(feature = "lfs")]
    pub use odf_storage_lfs as lfs;
    #[cfg(feature = "s3")]
    pub use odf_storage_s3 as s3;
}

pub mod serde {
    pub use odf_metadata::serde::*;
}

pub mod utils {
    pub use odf_data_utils::*;
}
