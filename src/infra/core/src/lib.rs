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

// Re-exports
pub use kamu_core as domain;

mod engine;
pub mod ingest;
mod query;
mod repos;
pub mod testing; // TODO: Put under feature flag
mod use_cases;
pub mod utils;

mod compaction_service_impl;
mod dataset_changes_service_impl;
mod dataset_config;
mod dataset_layout;
mod dataset_ownership_service_inmem;
mod dependency_graph_repository_inmem;
mod dependency_graph_service_inmem;
mod messages;
mod provenance_service_impl;
mod pull_service_impl;
mod push_service_impl;
mod query_service_impl;
mod remote_aliases_registry_impl;
mod remote_repository_registry_impl;
mod reset_service_impl;
mod resource_loader_impl;
mod search_service_impl;
mod sync_service_impl;
mod transform_service_impl;
mod verification_service_impl;

pub use compaction_service_impl::*;
pub use dataset_changes_service_impl::*;
pub use dataset_config::*;
pub use dataset_layout::*;
pub use dataset_ownership_service_inmem::*;
pub use dependency_graph_repository_inmem::*;
pub use dependency_graph_service_inmem::*;
pub use engine::*;
pub use ingest::*;
pub use messages::*;
pub use provenance_service_impl::*;
pub use pull_service_impl::*;
pub use push_service_impl::*;
pub use query_service_impl::*;
pub use remote_aliases_registry_impl::*;
pub use remote_repository_registry_impl::*;
pub use repos::*;
pub use reset_service_impl::*;
pub use resource_loader_impl::*;
pub use search_service_impl::*;
pub use sync_service_impl::*;
pub use transform_service_impl::*;
pub use use_cases::*;
pub use verification_service_impl::*;
