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

simple_string_scalar!(ResourceID, odf::metadata::resource::ResourceID);
simple_string_scalar!(ResourceName, odf::metadata::resource::ResourceName);
simple_string_scalar!(ResourceSelectorName, kamu_resources::ResourceSelectorName);
simple_string_scalar!(
    ResourceTypeSelectorRaw,
    kamu_resources::ResourceTypeSelectorRaw
);
simple_string_scalar!(TypeName, odf::metadata::resource::TypeName);
simple_string_scalar!(TypeUri, odf::metadata::resource::TypeUri);
simple_string_scalar!(TypeRef, odf::metadata::resource::TypeRef);
simple_string_scalar!(Did, odf::metadata::formats::Did, from_did_str);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
