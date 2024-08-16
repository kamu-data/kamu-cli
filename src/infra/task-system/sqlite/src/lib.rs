// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![feature(error_generic_member_access)]
#![feature(hash_set_entry)]
#![feature(let_chains)]

// Re-exports
pub use kamu_task_system as domain;

mod sqlite_task_system_event_store;

pub use sqlite_task_system_event_store::*;
