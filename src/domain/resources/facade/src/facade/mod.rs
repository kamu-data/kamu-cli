// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod graphql;
mod local;
mod resource_account_resolver;
mod resource_facade;
mod resource_facade_errors;
/// ODF-shaped selector types, staged in ahead of the surface swap.
///
/// Deliberately *not* glob-exported: `ResourceRef` and `ResourceSelector` here
/// mean something different from the same names in [`resource_facade`], and
/// both must coexist until stage 3b retires the old pair. Reach for these as
/// `selectors::ResourceSelector`.
pub mod resource_selectors;

pub use graphql::*;
pub use local::*;
pub use resource_account_resolver::*;
pub use resource_facade::*;
pub use resource_facade_errors::*;
pub use resource_selectors as selectors;
