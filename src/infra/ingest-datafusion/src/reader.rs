// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;
use std::sync::Arc;

use datafusion::prelude::*;
use internal_error::InternalError;
use opendatafabric::ReadStep;

///////////////////////////////////////////////////////////////////////////////

/// A common interface for readers that implement support for various formats
/// defined in [opendatafabric::ReadStep].
#[async_trait::async_trait]
pub trait Reader: Send + Sync {
    async fn read(
        &self,
        ctx: &SessionContext,
        path: &Path,
        conf: &ReadStep,
    ) -> Result<DataFrame, ReadError>;
}

///////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ReadError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

///////////////////////////////////////////////////////////////////////////////

// TODO: Replace with DI
pub fn get_reader_for(conf: &ReadStep) -> Arc<dyn Reader> {
    use crate::readers::*;

    match conf {
        ReadStep::Csv(_) => Arc::new(ReaderCsv {}),
        ReadStep::JsonLines(_) => todo!(),
        ReadStep::GeoJson(_) => todo!(),
        ReadStep::EsriShapefile(_) => todo!(),
        ReadStep::Parquet(_) => todo!(),
    }
}
