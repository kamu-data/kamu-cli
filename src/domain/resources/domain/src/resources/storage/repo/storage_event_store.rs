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
use internal_error::InternalError;

use crate::{ResourceID, ResourceIDStream, ResourceName, StorageState};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait StorageEventStore: EventStore<StorageState> {
    async fn new_storage_id(&self) -> Result<ResourceID, InternalError>;

    async fn get_storage_id_by_name(
        &self,
        name: &ResourceName,
    ) -> Result<Option<ResourceID>, InternalError>;

    fn list_storages(&self, pagination: PaginationOpts) -> ResourceIDStream<'_>;

    async fn get_count_storages(&self) -> Result<usize, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
