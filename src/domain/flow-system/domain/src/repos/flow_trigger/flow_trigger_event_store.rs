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
pub trait FlowTriggerEventStore: EventStore<FlowTriggerState> {
    /// Returns unique values of dataset IDs associated with update configs
    async fn list_dataset_ids(
        &self,
        pagination: &PaginationOpts,
    ) -> Result<Vec<odf::DatasetID>, InternalError>;

    /// Returns all existing flow bindings, where triggers are active
    fn stream_all_active_flow_bindings(&self) -> FlowBindingStream;

    /// Returns all bindings for a given dataset ID where triggers are defined
    /// regardless of status
    async fn all_trigger_bindings_for_dataset_flows(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Vec<FlowBinding>, InternalError>;

    /// Returns all bindings of system flows where triggers are defined
    /// regardless of status
    async fn all_trigger_bindings_for_system_flows(
        &self,
    ) -> Result<Vec<FlowBinding>, InternalError>;

    async fn all_dataset_ids_count(&self) -> Result<usize, InternalError>;

    /// Checks if there are any active triggers for the given list of datasets
    async fn has_active_triggers_for_datasets(
        &self,
        dataset_ids: &[odf::DatasetID],
    ) -> Result<bool, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
