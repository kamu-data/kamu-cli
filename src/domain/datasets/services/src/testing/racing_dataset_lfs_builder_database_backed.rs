// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{Catalog, component, interface};

use super::{DatasetReferenceRace, RacingDataset};
use crate::{DatasetLfsBuilderDatabaseBackedImpl, MetadataChainDbBackedConfig};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Builds datasets like `DatasetLfsBuilderDatabaseBackedImpl`, registered in
/// its place, as [`RacingDataset`]s
pub struct RacingDatasetLfsBuilderDatabaseBacked {
    inner: DatasetLfsBuilderDatabaseBackedImpl,
    race: Arc<DatasetReferenceRace>,
}

#[component(pub)]
#[interface(dyn odf::dataset::DatasetLfsBuilder)]
impl RacingDatasetLfsBuilderDatabaseBacked {
    pub fn new(
        catalog: Catalog,
        config: Arc<MetadataChainDbBackedConfig>,
        race: Arc<DatasetReferenceRace>,
    ) -> Self {
        Self {
            inner: DatasetLfsBuilderDatabaseBackedImpl::new(catalog, config),
            race,
        }
    }
}

impl odf::dataset::DatasetLfsBuilder for RacingDatasetLfsBuilderDatabaseBacked {
    fn build_lfs_dataset(
        &self,
        dataset_id: &odf::DatasetID,
        layout: odf::dataset::DatasetLayout,
    ) -> Arc<dyn odf::Dataset> {
        RacingDataset::wrap(
            self.inner.build_lfs_dataset(dataset_id, layout),
            dataset_id,
            &self.race,
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
