// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::TypeUri;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourceSpecViewDispatcher: Send + Sync {
    fn schema(&self) -> &'static TypeUri;

    fn reveal_spec(&self, spec: serde_json::Value) -> Result<serde_json::Value, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
