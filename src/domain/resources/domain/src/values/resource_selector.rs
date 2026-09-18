// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Matches zero or many resources using identity and label filters — a SQL
/// `LIKE` name pattern plus label filters, where
/// [`ResourceRef`](crate::ResourceRef) names exactly one resource by exact
/// name.
///
/// Several selectors in one call act as a logical OR, which is what lets one
/// call span several resource types.
///
/// `r#type` is optional and `None` means *any type*. This was once a kamu-local
/// superset of the spec — and the sole reason this was a twin struct rather
/// than an alias — until ODF adopted the optional type upstream.
///
/// `did` is forward-reserved in ODF for when datasets and accounts become
/// resources; no repository can resolve by it today, so the facade rejects a
/// selector carrying one rather than ignoring it.
pub type ResourceSelector = odf::metadata::resource::ResourceSelector;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
