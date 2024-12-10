// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![feature(assert_matches)]
#![feature(try_blocks)]
#![feature(box_patterns)]
#![feature(exit_status_error)]
#![feature(error_generic_member_access)]
#![feature(trait_upcasting)]
#![feature(let_chains)]
#![feature(iter_collect_into)]

// Re-exports
pub use kamu_core as domain;

mod engine;
pub mod ingest;
mod query;
mod repos;
#[cfg(any(feature = "testing", test))]
pub mod testing;
mod transform;
mod use_cases;
pub mod utils;

mod compaction_service_impl;
mod dataset_changes_service_impl;
mod dataset_config;
mod dataset_layout;
mod dataset_ownership_service_inmem;
mod dataset_registry_repo_bridge;
mod provenance_service_impl;
mod pull_request_planner_impl;
mod push_request_planner_impl;
mod query_service_impl;
mod remote_alias_resolver_impl;
mod remote_aliases_registry_impl;
mod remote_repository_registry_impl;
mod remote_status_service_impl;
mod reset_service_impl;
mod resource_loader_impl;
mod search_service_impl;
mod sync_request_builder;
mod sync_service_impl;
mod verification_service_impl;
mod watermark_service_impl;

pub use compaction_service_impl::*;
pub use dataset_changes_service_impl::*;
pub use dataset_config::*;
pub use dataset_layout::*;
pub use dataset_ownership_service_inmem::*;
pub use dataset_registry_repo_bridge::*;
pub use engine::*;
pub use ingest::*;
pub use provenance_service_impl::*;
pub use pull_request_planner_impl::*;
pub use push_request_planner_impl::*;
pub use query_service_impl::*;
pub use remote_alias_resolver_impl::*;
pub use remote_aliases_registry_impl::*;
pub use remote_repository_registry_impl::*;
pub use remote_status_service_impl::*;
pub use repos::*;
pub use reset_service_impl::*;
pub use resource_loader_impl::*;
pub use search_service_impl::*;
pub use sync_request_builder::*;
pub use sync_service_impl::*;
pub use transform::*;
pub use use_cases::*;
pub use verification_service_impl::*;
pub use watermark_service_impl::*;
