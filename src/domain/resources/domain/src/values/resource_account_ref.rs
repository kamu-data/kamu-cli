// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A reference to an account by id, name, or both — unresolved input for
/// manifest headers and facade account selectors. Resolution to a concrete
/// account happens downstream (see `ResourceAccountResolver` in the facade
/// crate); this type only carries what the caller supplied.
pub type ResourceAccountRef = odf::metadata::auth::AccountRef;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
