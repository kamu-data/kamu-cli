// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryFrom;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

simple_string_scalar!(
    /// Unique and stable identifier of a dataset (`did:odf:...`)
    DatasetID,
    odf::DatasetID,
    from_did_str
);
simple_string_scalar!(
    /// Name of a dataset, unique within its owner account
    DatasetName,
    odf::DatasetName
);
simple_string_scalar!(
    /// Dataset name, optionally prefixed with the owner account
    /// (`account/dataset`)
    DatasetAlias,
    odf::DatasetAlias
);
simple_string_scalar!(
    /// Reference to a local or remote dataset by ID, alias or URL
    DatasetRefAny,
    odf::DatasetRefAny
);
simple_string_scalar!(
    /// Reference to a local dataset by ID or alias
    DatasetRef,
    odf::DatasetRef
);
simple_string_scalar!(
    /// Reference to a remote dataset by ID, alias or URL
    DatasetRefRemote,
    odf::DatasetRefRemote
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
