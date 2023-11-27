// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![feature(provide_any)]
#![feature(error_generic_member_access)]

// Re-exports
pub use event_sourcing::*;
pub use internal_error::*;

pub mod aggregates;
pub mod entities;
pub mod repos;
pub mod services;

pub use aggregates::*;
pub use entities::*;
pub use repos::*;
pub use services::*;
