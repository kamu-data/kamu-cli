// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use event_sourcing::EventStore;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowConfigurationEventStore: EventStore<FlowConfigurationState> {
    /// Returns unique values of dataset IDs associated with update configs
    async fn list_dataset_ids(
        &self,
        pagination: &PaginationOpts,
    ) -> Result<Vec<odf::DatasetID>, InternalError>;

    async fn all_dataset_ids_count(&self) -> Result<usize, InternalError>;

    /// Returns all existing flow bindings, where configurations exist
    fn stream_all_existing_flow_bindings(&self) -> FlowBindingStream;

    /// Returns all bindings for a given dataset ID where configs exist
    async fn all_bindings_for_dataset_flows(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Vec<FlowBinding>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
