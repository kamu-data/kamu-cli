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
pub use kamu_accounts as domain;

mod authentication_service_impl;
mod login_password_auth_provider;
mod predefined_accounts_registrator;

pub use authentication_service_impl::*;
pub use login_password_auth_provider::*;
pub use predefined_accounts_registrator::*;
