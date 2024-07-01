// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Re-exports
pub use kamu_dataset_env_vars as domain;

mod dataset_env_var_service_impl;
mod dataset_env_var_service_static_impl;

pub use dataset_env_var_service_impl::*;
pub use dataset_env_var_service_static_impl::*;
