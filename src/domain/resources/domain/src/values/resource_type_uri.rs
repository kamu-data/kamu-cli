// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Canonical schema URL that identifies a resource type, e.g.
/// `https://opendatafabric.org/schemas/config/v1alpha1/VariableSet`.
///
/// This is the ODF-canonical representation of what the framework calls the
/// resource "schema". See [`crate::ResourceSchemaId`] for the parsed lens over
/// a `TypeUri` (base/context/version/name decomposition).
pub type TypeUri = odf::metadata::resource::TypeUri;

/// CRD-style short type name of a resource, e.g. `VariableSet`.
pub type TypeName = odf::metadata::resource::TypeName;

/// A label/annotation key: either a short [`TypeName`] (e.g. `env`) or a full
/// [`TypeUri`] schema URI.
pub type TypeRef = odf::metadata::resource::TypeRef;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
