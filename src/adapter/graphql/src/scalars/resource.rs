// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

simple_string_scalar!(
    /// Unique and stable identifier of a resource
    ResourceID,
    odf::metadata::resources::ResourceID
);
simple_string_scalar!(
    /// Name of a resource
    ResourceName,
    odf::metadata::resources::ResourceName
);
simple_string_scalar!(
    /// Short name that selects a resource type (e.g. in `kind` filters)
    ResourceSelectorName,
    kamu_resources::ResourceSelectorName
);
simple_string_scalar!(
    /// Resource type selector as authored: a selector name, an alias, a type
    /// name or a schema URI
    ResourceTypeSelectorRaw,
    kamu_resources::ResourceTypeSelectorRaw
);
simple_string_scalar!(
    /// Name of a resource type
    TypeName,
    odf::metadata::resources::TypeName
);
simple_string_scalar!(
    /// URI of a resource type schema
    TypeUri,
    odf::metadata::resources::TypeUri
);
simple_string_scalar!(
    /// Reference to a resource type by name or by schema URI
    TypeRef,
    odf::metadata::resources::TypeRef
);
simple_string_scalar!(
    /// Decentralized identifier (DID)
    Did,
    odf::metadata::formats::Did,
    from_did_str
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
