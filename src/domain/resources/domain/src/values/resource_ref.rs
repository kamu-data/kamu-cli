// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A reference to exactly one resource, by exact name or by id — unresolved
/// input for facade operations that act on a single resource.
///
/// Contrast [`ResourceSelector`](crate::ResourceSelector), whose `name` is a
/// SQL `LIKE` pattern and which therefore matches zero or many. Resolution of
/// the type and account happens downstream, in the facade; this type only
/// carries what the caller supplied.
///
/// `did` is forward-reserved in ODF for when datasets and accounts become
/// resources — no repository can resolve by it today, so the facade rejects it
/// as unsupported rather than ignoring it.
pub type ResourceRef = odf::metadata::resource::ResourceRef;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
