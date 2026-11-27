// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources as domain;
pub use messaging_outbox;
pub use paste;

mod controller_runtime;
mod controllers;
#[macro_use]
mod crud_dispatchers;
mod dependencies;
mod event_stores;
mod message_handlers;
#[macro_use]
mod resource_extension_schemas;
mod resources;
mod retention;
mod services;
mod use_cases;

// After the modules whose macros it uses
#[cfg(feature = "testing")]
pub mod testing;

pub use controller_runtime::*;
pub use controllers::*;
pub use crud_dispatchers::*;
pub use dependencies::*;
pub use event_stores::*;
pub use message_handlers::*;
pub use resource_extension_schemas::*;
pub use retention::*;
pub use services::*;
pub use use_cases::*;
