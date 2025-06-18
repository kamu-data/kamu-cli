// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct FlowBinding {
    pub flow_type: String,
    pub scope: FlowScope,
}

impl FlowBinding {
    pub fn new_dataset(dataset_id: odf::DatasetID, flow_type: &str) -> Self {
        Self {
            flow_type: flow_type.to_string(),
            scope: FlowScope::Dataset { dataset_id },
        }
    }

    pub fn new_system(flow_type: &str) -> Self {
        Self {
            flow_type: flow_type.to_string(),
            scope: FlowScope::System,
        }
    }

    pub fn dataset_id_or_die(&self) -> Result<odf::DatasetID, InternalError> {
        let FlowScope::Dataset { dataset_id } = &self.scope else {
            return InternalError::bail("Expecting dataset flow binding scope");
        };
        Ok(dataset_id.clone())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum FlowScope {
    Dataset { dataset_id: odf::DatasetID },
    System,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
