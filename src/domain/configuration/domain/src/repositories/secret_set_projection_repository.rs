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

use crate::{ReplaceProjectionEntriesError, SecretSetEntry};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait SecretSetProjectionRepository: Send + Sync {
    async fn find_entry(
        &self,
        resource_uid: &ResourceUID,
        resource_generation: u64,
        key: &str,
    ) -> Result<Option<SecretSetEntry>, InternalError>;

    async fn get_entries(
        &self,
        resource_uid: &ResourceUID,
        resource_generation: u64,
    ) -> Result<Vec<SecretSetEntry>, InternalError>;

    async fn get_latest_entries(
        &self,
        resource_uid: &ResourceUID,
    ) -> Result<Vec<SecretSetEntry>, InternalError>;

    async fn get_latest_entries_before_generation(
        &self,
        resource_uid: &ResourceUID,
        resource_generation: u64,
    ) -> Result<Vec<SecretSetEntry>, InternalError>;

    async fn replace_entries(
        &self,
        resource_uid: &ResourceUID,
        resource_generation: u64,
        entries: &[SecretSetEntry],
    ) -> Result<(), ReplaceProjectionEntriesError>;

    async fn cleanup_entries_before_generation(
        &self,
        resource_uid: &ResourceUID,
        resource_generation: u64,
    ) -> Result<(), InternalError>;

    async fn delete_all_entries(&self, resource_uid: &ResourceUID) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
