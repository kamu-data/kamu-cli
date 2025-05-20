// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;

use internal_error::InternalError;
use odf_metadata::{DatasetHandle, DatasetID};
use tokio_stream::Stream;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type DatasetHandleStream<'a> =
    Pin<Box<dyn Stream<Item = Result<DatasetHandle, InternalError>> + Send + 'a>>;

pub type DatasetIDStream<'a> =
    Pin<Box<dyn Stream<Item = Result<DatasetID, InternalError>> + Send + 'a>>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
