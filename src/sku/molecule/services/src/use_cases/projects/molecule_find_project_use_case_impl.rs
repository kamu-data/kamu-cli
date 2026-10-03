// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::PaginationOpts;
use internal_error::ResultIntoInternal;
use kamu_accounts::LoggedAccount;
use kamu_molecule_domain::{
    molecule_project_search_schema as project_schema,
    molecule_search_schema_common as molecule_schema,
    *,
};
use kamu_search::*;

use crate::MoleculeProjectsService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeFindProjectUseCase)]
pub struct MoleculeFindProjectUseCaseImpl {
    catalog: dill::Catalog,
    projects_service: Arc<dyn MoleculeProjectsService>,
    search_service: Arc<dyn SearchService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeFindProjectUseCaseImpl {
    async fn find_from_source(
        &self,
        molecule_subject: &LoggedAccount,
        ocl_id: &OclId,
    ) -> Result<Option<MoleculeProject>, MoleculeFindProjectError> {
        // Gain read access to projects dataset
        let projects_reader = self
            .projects_service
            .reader(&molecule_subject.account_name)
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeFindProjectError>)?;

        // Query for the project by ocl_id
        let maybe_project = projects_reader
            .changelog_entry_by_ocl_id(ocl_id)
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeFindProjectError>)?
            .map(MoleculeProject::from_json)
            .transpose()?;

        Ok(maybe_project)
    }

    async fn find_from_search(
        &self,
        molecule_subject: &LoggedAccount,
        ocl_id: &OclId,
    ) -> Result<Option<MoleculeProject>, MoleculeFindProjectError> {
        let ctx = SearchContext {
            catalog: &self.catalog,
            security: SearchSecurityContext::Restricted {
                current_principal_ids: vec![molecule_subject.account_id.to_string()],
            },
        };

        let search_results = self
            .search_service
            .listing_search(
                ctx,
                ListingSearchRequest {
                    entity_schemas: vec![project_schema::SCHEMA_NAME],
                    source: SearchRequestSourceSpec::All,
                    filter: Some(field_eq_str(
                        molecule_schema::fields::OCL_ID,
                        ocl_id.as_ref(),
                    )),
                    sort: sort!(project_schema::fields::SYMBOL, asc),
                    page: Some(PaginationOpts {
                        offset: 0,
                        limit: 1,
                    })
                    .into(),
                },
            )
            .await
            .int_err()?;

        search_results
            .hits
            .into_iter()
            .next()
            .map(|hit| MoleculeProject::from_search_index_json(hit.id, hit.source))
            .transpose()
            .map_err(Into::into)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
#[common_macros::method_names_consts]
impl MoleculeFindProjectUseCase for MoleculeFindProjectUseCaseImpl {
    #[tracing::instrument(level = "debug", name = MoleculeFindProjectUseCaseImpl_execute, skip_all, fields(?mode, ocl_id))]
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        mode: MoleculeViewProjectsMode,
        ocl_id: OclId,
    ) -> Result<Option<MoleculeProject>, MoleculeFindProjectError> {
        match mode {
            MoleculeViewProjectsMode::LatestSource => {
                self.find_from_source(molecule_subject, &ocl_id).await
            }
            MoleculeViewProjectsMode::LatestProjection => {
                self.find_from_search(molecule_subject, &ocl_id).await
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
