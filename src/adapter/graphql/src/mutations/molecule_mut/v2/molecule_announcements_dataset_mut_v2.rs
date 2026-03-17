// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_molecule_domain::{MoleculeAnnouncementPayloadRecord, MoleculeCreateAnnouncementUseCase};
use time_source::SystemTimeSource;

use crate::molecule::molecule_subject;
use crate::prelude::*;
use crate::queries::molecule::v2::{MoleculeAccessLevel, MoleculeProjectV2};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeAnnouncementsDatasetMutV2 {
    project: Arc<MoleculeProjectV2>,
}

impl MoleculeAnnouncementsDatasetMutV2 {
    pub fn new(project: Arc<MoleculeProjectV2>) -> Self {
        Self { project }
    }
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeAnnouncementsDatasetMutV2 {
    /// Creates an announcement record for the project.
    #[tracing::instrument(level = "info", name = MoleculeAnnouncementsDatasetMutV2_create, skip_all)]
    async fn create(
        &self,
        ctx: &Context<'_>,
        headline: String,
        body: String,
        #[graphql(desc = "List of dataset DIDs to link")] attachments: Option<Vec<DatasetID<'_>>>,
        access_level: MoleculeAccessLevel,
        change_by: String,
        categories: Vec<String>,
        tags: Vec<String>,
    ) -> Result<CreateAnnouncementResult> {
        let molecule_subject = molecule_subject(ctx)?;

        let (time_source, molecule_create_announcement_use_case) = from_catalog_n!(
            ctx,
            dyn SystemTimeSource,
            dyn MoleculeCreateAnnouncementUseCase
        );

        let event_time = time_source.now();

        let announcement_record = MoleculeAnnouncementPayloadRecord {
            announcement_id: uuid::Uuid::new_v4(),
            headline,
            body,
            attachments: attachments
                .unwrap_or_default()
                .into_iter()
                .map(Into::into)
                .collect(),
            change_by,
            access_level,
            categories: Some(categories),
            tags: Some(tags),
        };

        use kamu_molecule_domain::MoleculeCreateAnnouncementError as E;

        match molecule_create_announcement_use_case
            .execute(
                &molecule_subject,
                &self.project.entity,
                Some(event_time),
                announcement_record,
            )
            .await
        {
            Ok(create_res) => Ok(CreateAnnouncementResult::Success(
                CreateAnnouncementSuccess {
                    announcement_id: create_res.new_announcement_id.to_string(),
                },
            )),
            Err(E::NotFoundAttachments(e)) => Ok(CreateAnnouncementResult::InvalidAttachment(
                CreateAnnouncementErrorInvalidAttachment {
                    message: e.to_string(),
                },
            )),
            Err(E::QuotaExceeded(e)) => Ok(CreateAnnouncementResult::QuotaExceeded(
                CreateAnnouncementErrorQuotaExceeded {
                    message: e.to_string(),
                },
            )),
            Err(E::Access(e)) => Err(e.into()),
            Err(e @ E::Internal(_)) => Err(e.int_err().into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(
    field(name = "is_success", ty = "bool"),
    field(name = "message", ty = "String")
)]
pub enum CreateAnnouncementResult {
    Success(CreateAnnouncementSuccess),
    InvalidAttachment(CreateAnnouncementErrorInvalidAttachment),
    QuotaExceeded(CreateAnnouncementErrorQuotaExceeded),
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct CreateAnnouncementSuccess {
    /// ID of the newly-created announcement
    pub announcement_id: String,
}
#[ComplexObject]
impl CreateAnnouncementSuccess {
    async fn is_success(&self) -> bool {
        true
    }
    async fn message(&self) -> String {
        String::new()
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct CreateAnnouncementErrorInvalidAttachment {
    pub(crate) message: String,
}
#[ComplexObject]
impl CreateAnnouncementErrorInvalidAttachment {
    async fn is_success(&self) -> bool {
        false
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct CreateAnnouncementErrorQuotaExceeded {
    pub(crate) message: String,
}
#[ComplexObject]
impl CreateAnnouncementErrorQuotaExceeded {
    async fn is_success(&self) -> bool {
        false
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
