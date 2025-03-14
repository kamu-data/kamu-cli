// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// TODO: Extract into `kamu-search-services` crate after remote repositories are
// extracted from core
mod search_service_remote_impl;

pub use search_service_remote_impl::*;
