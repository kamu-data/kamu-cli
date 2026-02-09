// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::sync::Arc;

use crate::CreateDatasetResult;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct ResolvedDataset {
    dataset: Arc<dyn odf::Dataset>,
    handle: odf::DatasetHandle,
}

impl ResolvedDataset {
    pub fn new(dataset: Arc<dyn odf::Dataset>, handle: odf::DatasetHandle) -> Self {
        Self { dataset, handle }
    }

    pub fn from_created(create_dataset_result: &CreateDatasetResult) -> Self {
        Self {
            dataset: create_dataset_result.dataset.clone(),
            handle: create_dataset_result.dataset_handle.clone(),
        }
    }

    pub fn from_stored(
        store_dataset_result: &odf::dataset::StoreDatasetResult,
        dataset_alias: &odf::DatasetAlias,
    ) -> Self {
        Self {
            dataset: store_dataset_result.dataset.clone(),
            handle: odf::DatasetHandle::new(
                store_dataset_result.dataset_id.clone(),
                dataset_alias.clone(),
                store_dataset_result.dataset_kind,
            ),
        }
    }

    /// Detaches this dataset from any transaction references
    pub fn detach_from_transaction(&self) {
        self.dataset.detach_from_transaction();
    }

    #[inline]
    pub fn get_id(&self) -> &odf::DatasetID {
        &self.handle.id
    }

    #[inline]
    pub fn get_kind(&self) -> odf::DatasetKind {
        self.handle.kind
    }

    #[inline]
    pub fn get_alias(&self) -> &odf::DatasetAlias {
        &self.handle.alias
    }

    #[inline]
    pub fn get_handle(&self) -> &odf::DatasetHandle {
        &self.handle
    }

    #[inline]
    pub fn take_handle(self) -> odf::DatasetHandle {
        self.handle
    }
}

impl std::ops::Deref for ResolvedDataset {
    type Target = Arc<dyn odf::Dataset>;
    fn deref(&self) -> &Self::Target {
        &self.dataset
    }
}

impl std::fmt::Debug for ResolvedDataset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.handle.fmt(f)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct WriteCheckedDataset<'a>(Cow<'a, ResolvedDataset>);

impl WriteCheckedDataset<'_> {
    pub fn from_ref(dataset: &ResolvedDataset) -> WriteCheckedDataset<'_> {
        WriteCheckedDataset(Cow::Borrowed(dataset))
    }

    pub fn from_owned(dataset: ResolvedDataset) -> WriteCheckedDataset<'static> {
        WriteCheckedDataset(Cow::Owned(dataset))
    }

    pub fn into_inner(self) -> ResolvedDataset {
        self.0.into_owned()
    }
}

impl std::ops::Deref for WriteCheckedDataset<'_> {
    type Target = ResolvedDataset;
    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct ReadCheckedDataset<'a>(Cow<'a, ResolvedDataset>);

impl ReadCheckedDataset<'_> {
    pub fn from_ref(dataset: &ResolvedDataset) -> ReadCheckedDataset<'_> {
        ReadCheckedDataset(Cow::Borrowed(dataset))
    }

    pub fn from_owned(dataset: ResolvedDataset) -> ReadCheckedDataset<'static> {
        ReadCheckedDataset(Cow::Owned(dataset))
    }

    pub fn into_inner(self) -> ResolvedDataset {
        self.0.into_owned()
    }
}

impl std::ops::Deref for ReadCheckedDataset<'_> {
    type Target = ResolvedDataset;
    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

// Writing presumes Read access as well
impl<'a> From<WriteCheckedDataset<'a>> for ReadCheckedDataset<'a> {
    fn from(v: WriteCheckedDataset<'a>) -> Self {
        ReadCheckedDataset(v.0)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
