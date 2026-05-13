// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_resources::ResourceUID;

use crate::{ReplaceProjectionEntriesError, VariableSetEntry};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait VariableSetProjectionRepository: Send + Sync {
    async fn replace_entries(
        &self,
        resource_uid: &ResourceUID,
        resource_generation: u64,
        entries: &[VariableSetEntry],
    ) -> Result<(), ReplaceProjectionEntriesError>;

    async fn find_entry(
        &self,
        resource_uid: &ResourceUID,
        resource_generation: u64,
        key: &str,
    ) -> Result<Option<VariableSetEntry>, InternalError>;

    async fn get_entries(
        &self,
        resource_uid: &ResourceUID,
        resource_generation: u64,
    ) -> Result<Vec<VariableSetEntry>, InternalError>;

    async fn get_latest_entries_before_generation(
        &self,
        resource_uid: &ResourceUID,
        resource_generation: u64,
    ) -> Result<Vec<VariableSetEntry>, InternalError>;

    async fn cleanup_entries_before_generation(
        &self,
        resource_uid: &ResourceUID,
        resource_generation: u64,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
