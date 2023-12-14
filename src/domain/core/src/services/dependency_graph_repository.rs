// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;

use futures::Stream;
use internal_error::InternalError;
use opendatafabric::DatasetID;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DependencyGraphRepository: Sync + Send {
    fn list_dependencies_of_all_datasets(&self) -> DatasetDependenciesIDStream;
}

/////////////////////////////////////////////////////////////////////////////////////////

pub type DatasetDependenciesIDStream<'a> =
    Pin<Box<dyn Stream<Item = Result<(DatasetID, DatasetID), InternalError>> + Send + 'a>>;

/////////////////////////////////////////////////////////////////////////////////////////
