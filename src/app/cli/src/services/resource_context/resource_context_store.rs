// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::resource_context;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourceContextStore: Send + Sync {
    fn read_context_registry(
        &self,
        scope: resource_context::ResourceContextStoreScope,
    ) -> Result<resource_context::ResourceContextRegistry, InternalError>;

    fn write_context_registry(
        &self,
        scope: resource_context::ResourceContextStoreScope,
        registry: &resource_context::ResourceContextRegistry,
    ) -> Result<(), InternalError>;

    fn read_account_runtime_state(
        &self,
        scope: resource_context::ResourceContextStoreScope,
        account_name: &odf::AccountName,
    ) -> Result<resource_context::AccountContextRuntimeState, InternalError>;

    fn write_account_runtime_state(
        &self,
        scope: resource_context::ResourceContextStoreScope,
        account_name: &odf::AccountName,
        state: &resource_context::AccountContextRuntimeState,
    ) -> Result<(), InternalError>;

    fn read_current_context_state(
        &self,
        scope: resource_context::ResourceContextStoreScope,
        account_name: &odf::AccountName,
    ) -> Result<resource_context::CurrentResourceContextState, InternalError>;

    fn write_current_context_state(
        &self,
        scope: resource_context::ResourceContextStoreScope,
        account_name: &odf::AccountName,
        state: &resource_context::CurrentResourceContextState,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
