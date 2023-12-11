// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Borrow;
use std::hash::{Hash, Hasher};

use kamu_flow_system::DatasetFlowType;
use opendatafabric::DatasetID;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Eq, PartialEq, Hash, Clone)]
pub(crate) struct OwnedDatasetFlowKey {
    dataset_id: DatasetID,
    flow_type: DatasetFlowType,
}

impl OwnedDatasetFlowKey {
    pub fn new(dataset_id: DatasetID, flow_type: DatasetFlowType) -> Self {
        Self {
            dataset_id,
            flow_type,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Copy, Clone, Hash, PartialEq, Eq)]
pub(crate) struct BorrowedDatasetFlowKey<'a> {
    dataset_id: &'a DatasetID,
    flow_type: DatasetFlowType,
}

impl<'a> BorrowedDatasetFlowKey<'a> {
    pub fn new(dataset_id: &'a DatasetID, flow_type: DatasetFlowType) -> Self {
        Self {
            dataset_id,
            flow_type,
        }
    }

    pub fn as_trait(&self) -> &dyn BorrowedDatasetFlowKeyHelper {
        self as &dyn BorrowedDatasetFlowKeyHelper
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) trait BorrowedDatasetFlowKeyHelper {
    fn borrowed_key(&self) -> BorrowedDatasetFlowKey;
}

impl BorrowedDatasetFlowKeyHelper for OwnedDatasetFlowKey {
    fn borrowed_key(&self) -> BorrowedDatasetFlowKey {
        BorrowedDatasetFlowKey {
            dataset_id: &self.dataset_id,
            flow_type: self.flow_type,
        }
    }
}

impl BorrowedDatasetFlowKeyHelper for BorrowedDatasetFlowKey<'_> {
    fn borrowed_key(&self) -> BorrowedDatasetFlowKey {
        *self
    }
}

impl<'a> Borrow<dyn BorrowedDatasetFlowKeyHelper + 'a> for OwnedDatasetFlowKey {
    fn borrow(&self) -> &(dyn BorrowedDatasetFlowKeyHelper + 'a) {
        self
    }
}

impl Eq for (dyn BorrowedDatasetFlowKeyHelper + '_) {}

impl PartialEq for (dyn BorrowedDatasetFlowKeyHelper + '_) {
    fn eq(&self, other: &dyn BorrowedDatasetFlowKeyHelper) -> bool {
        self.borrowed_key().eq(&other.borrowed_key())
    }
}

impl<'a> Hash for (dyn BorrowedDatasetFlowKeyHelper + 'a) {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.borrowed_key().hash(state)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
