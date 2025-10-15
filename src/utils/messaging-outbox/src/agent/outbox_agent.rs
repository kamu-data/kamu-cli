// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_utils::BackgroundAgent;
use init_on_startup::InitOnStartup;
use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait OutboxAgent: BackgroundAgent + InitOnStartup {
    async fn run_while_has_tasks(&self) -> Result<(), InternalError>;

    // To be used by tests only!
    async fn run_single_iteration_only(&self) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
