// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![feature(let_chains)]

// Re-exports
pub use kamu_datasets as domain;

mod dataset_env_var_service_impl;
mod dataset_env_var_service_static_impl;
mod dataset_key_value_service_impl;
mod dataset_key_value_service_static_impl;

pub use dataset_env_var_service_impl::*;
pub use dataset_env_var_service_static_impl::*;
pub use dataset_key_value_service_impl::*;
pub use dataset_key_value_service_static_impl::*;
