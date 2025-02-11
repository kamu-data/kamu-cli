// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use dill::{component, interface};
use internal_error::ErrorIntoInternal;
use kamu_accounts::{AnonymousAccountReason, CurrentAccountSubject};
use kamu_core::TenancyConfig;
use s3_utils::S3Context;
use time_source::SystemTimeSource;
use url::Url;

use super::{DatasetStorageUnitLocalFs, DatasetStorageUnitS3};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetStorageUnitFactoryImpl {
    system_time_source: Arc<dyn SystemTimeSource>,
}

#[component(pub)]
#[interface(dyn odf::dataset::DatasetStorageUnitFactory)]
impl DatasetStorageUnitFactoryImpl {
    pub fn new(system_time_source: Arc<dyn SystemTimeSource>) -> Self {
        Self { system_time_source }
    }

    fn get_local_fs(
        &self,
        datasets_root: &Path,
        tenancy_config: TenancyConfig,
    ) -> Arc<dyn odf::DatasetStorageUnit> {
        Arc::new(DatasetStorageUnitLocalFs::new(
            PathBuf::from(datasets_root),
            Arc::new(CurrentAccountSubject::anonymous(
                AnonymousAccountReason::NoAuthenticationProvided,
            )),
            Arc::new(tenancy_config),
        ))
    }

    pub async fn get_s3_from_url(
        &self,
        base_url: Url,
        tenancy_config: TenancyConfig,
    ) -> Arc<dyn odf::DatasetStorageUnit> {
        let s3_context = S3Context::from_url(&base_url).await;
        Arc::new(DatasetStorageUnitS3::new(
            s3_context,
            Arc::new(CurrentAccountSubject::anonymous(
                AnonymousAccountReason::NoAuthenticationProvided,
            )),
            Arc::new(tenancy_config),
            None,
            None,
            self.system_time_source.clone(),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl odf::dataset::DatasetStorageUnitFactory for DatasetStorageUnitFactoryImpl {
    async fn get_storage_unit(
        &self,
        url: &Url,
        is_multi_tenant: bool,
    ) -> Result<Arc<dyn odf::DatasetStorageUnit>, odf::dataset::BuildDatasetStorageUnitError> {
        let tenancy_config = if is_multi_tenant {
            TenancyConfig::MultiTenant
        } else {
            TenancyConfig::SingleTenant
        };

        match url.scheme() {
            "file" => {
                let path = url
                    .to_file_path()
                    .map_err(|_| "Invalid file url".int_err())?
                    .join("datasets/");
                Ok(self.get_local_fs(&path, tenancy_config))
            }
            "s3" | "s3+http" | "s3+https" => {
                Ok(self.get_s3_from_url(url.clone(), tenancy_config).await)
            }

            // TODO: http (for simple protocol)
            _ => Err(odf::dataset::UnsupportedProtocolError {
                message: None,
                entity_kind: "repository",
                url: Box::new(url.clone()),
            }
            .into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
