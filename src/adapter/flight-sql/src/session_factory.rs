// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::prelude::SessionContext;
use tonic::Status;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type Token = String;

#[async_trait::async_trait]
#[allow(unused_variables)]
pub trait SessionFactory: Send + Sync {
    async fn authenticate(&self, username: &str, password: &str) -> Result<Token, Status> {
        Err(Status::unauthenticated("Invalid credentials!"))
    }

    async fn get_context(&self, token: &Token) -> Result<Arc<SessionContext>, Status> {
        Err(Status::unauthenticated("Invalid credentials!"))?
    }
}
