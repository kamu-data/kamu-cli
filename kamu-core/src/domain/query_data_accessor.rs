// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::prelude::SessionContext;

use super::InternalError;

/////////////////////////////////////////////////////////////////////////////////////////

pub trait QueryDataAccessor: Send + Sync {
    fn bind_object_store(&self, session_context: &SessionContext) -> Result<(), InternalError>;

    fn object_store_url(&self) -> url::Url;
}

/////////////////////////////////////////////////////////////////////////////////////////
