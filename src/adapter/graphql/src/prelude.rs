// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub(crate) use async_graphql::*;
pub(crate) use internal_error::*;

pub(crate) use crate::guards::*;
pub(crate) use crate::scalars::*;
pub(crate) use crate::utils::{from_catalog_n, GqlError};

pub(crate) type Result<T, E = GqlError> = ::core::result::Result<T, E>;
