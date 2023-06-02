// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::*;

use super::*;
use crate::scalars::*;

///////////////////////////////////////////////////////////////////////////////

pub struct Tasks;

#[Object]
impl Tasks {
    /// Returns a task by its ID
    #[allow(unused_variables)]
    async fn by_id(&self, _ctx: &Context<'_>, task_id: TaskID) -> Result<Option<Task>> {
        Ok(Some(Task::mock()))
    }
}

///////////////////////////////////////////////////////////////////////////////
