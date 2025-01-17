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

use crate::{DatasetFlowType, FlowKeyDataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Copy, Clone, Hash, PartialEq, Eq)]
pub struct BorrowedFlowKeyDataset<'a> {
    dataset_id: &'a odf::DatasetID,
    flow_type: DatasetFlowType,
}

impl<'a> BorrowedFlowKeyDataset<'a> {
    pub fn new(dataset_id: &'a odf::DatasetID, flow_type: DatasetFlowType) -> Self {
        Self {
            dataset_id,
            flow_type,
        }
    }

    pub fn as_trait(&self) -> &dyn BorrowedFlowKeyDatasetHelper {
        self as &dyn BorrowedFlowKeyDatasetHelper
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait BorrowedFlowKeyDatasetHelper {
    fn borrowed_key(&self) -> BorrowedFlowKeyDataset;
}

impl BorrowedFlowKeyDatasetHelper for FlowKeyDataset {
    fn borrowed_key(&self) -> BorrowedFlowKeyDataset {
        BorrowedFlowKeyDataset {
            dataset_id: &self.dataset_id,
            flow_type: self.flow_type,
        }
    }
}

impl BorrowedFlowKeyDatasetHelper for BorrowedFlowKeyDataset<'_> {
    fn borrowed_key(&self) -> BorrowedFlowKeyDataset {
        *self
    }
}

impl<'a> Borrow<dyn BorrowedFlowKeyDatasetHelper + 'a> for FlowKeyDataset {
    fn borrow(&self) -> &(dyn BorrowedFlowKeyDatasetHelper + 'a) {
        self
    }
}

impl Eq for (dyn BorrowedFlowKeyDatasetHelper + '_) {}

impl PartialEq for (dyn BorrowedFlowKeyDatasetHelper + '_) {
    fn eq(&self, other: &dyn BorrowedFlowKeyDatasetHelper) -> bool {
        self.borrowed_key().eq(&other.borrowed_key())
    }
}

impl Hash for (dyn BorrowedFlowKeyDatasetHelper + '_) {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.borrowed_key().hash(state);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
