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

simple_string_scalar!(DatasetID, odf::DatasetID, from_did_str);
simple_string_scalar!(DatasetName, odf::DatasetName);
simple_string_scalar!(DatasetAlias, odf::DatasetAlias);
simple_string_scalar!(DatasetRefAny, odf::DatasetRefAny);
simple_string_scalar!(DatasetRef, odf::DatasetRef);
simple_string_scalar!(DatasetRefRemote, odf::DatasetRefRemote);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
