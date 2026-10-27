// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use dill::*;
use engine::{TransformRequestExt, TransformRequestInputExt};
use internal_error::ErrorIntoInternal;
use kamu_core::*;
use kamu_datasets::ResolvedDatasetsMap;
use time_source::SystemTimeSource;

use super::get_transform_input_from_query_input;
use crate::get_transform_query_input;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TransformElaborationServiceImpl {
    time_source: Arc<dyn SystemTimeSource>,
}

/// Fixes what elaboration would otherwise read at call time
struct ElaborationPins {
    system_time: DateTime<Utc>,
    input_heads: HashMap<odf::DatasetID, odf::Multihash>,
}

#[component(pub)]
#[interface(dyn TransformElaborationService)]
impl TransformElaborationServiceImpl {
    pub fn new(time_source: Arc<dyn SystemTimeSource>) -> Self {
        Self { time_source }
    }

    async fn elaborate_preliminary_request(
        &self,
        preliminary_request: TransformPreliminaryRequestExt,
        maybe_pins: Option<&ElaborationPins>,
        datasets_map: &ResolvedDatasetsMap,
    ) -> Result<Option<TransformRequestExt>, TransformElaborateError> {
        use futures::{StreamExt, TryStreamExt};
        let inputs: Vec<_> = futures::stream::iter(preliminary_request.input_states)
            .then(|(input_decl, input_state)| {
                self.get_transform_input(input_decl, input_state, maybe_pins, datasets_map)
            })
            .try_collect()
            .await?;

        // Nothing to do?
        // Note that we're considering a schema here, as even if there is no data to
        // process we would like to run the transform to establish the schema of the
        // output.
        //
        // TODO: Detect the situation where inputs only had source updates and skip
        // running the engine
        if inputs
            .iter()
            .all(|i| i.data_slices.is_empty() && i.explicit_watermarks.is_empty())
            && preliminary_request.schema.is_some()
        {
            return Ok(None);
        }

        let final_request: TransformRequestExt = TransformRequestExt {
            dataset_handle: preliminary_request.dataset_handle,
            block_ref: preliminary_request.block_ref,
            head: preliminary_request.head,
            transform: preliminary_request.transform,
            transform_block: preliminary_request.transform_block,
            system_time: match maybe_pins {
                Some(pins) => pins.system_time,
                None => self.time_source.now(),
            },
            schema: preliminary_request.schema,
            prev_offset: preliminary_request.prev_offset,
            vocab: preliminary_request.vocab,
            inputs,
            prev_checkpoint: preliminary_request.prev_checkpoint,
        };

        Ok(Some(final_request))
    }

    async fn get_transform_input(
        &self,
        input_decl: odf::metadata::TransformInput,
        input_state: Option<odf::metadata::ExecuteTransformInput>,
        maybe_pins: Option<&ElaborationPins>,
        datasets_map: &ResolvedDatasetsMap,
    ) -> Result<TransformRequestInputExt, TransformElaborateError> {
        let input_alias = input_decl.alias.clone().unwrap();

        let maybe_input_head = match maybe_pins {
            Some(pins) => {
                let input_id = input_decl.dataset_ref.id().unwrap();
                let input_head = pins.input_heads.get(input_id).ok_or_else(|| {
                    format!("Stored transform plan has no head for input {input_id}").int_err()
                })?;
                Some(input_head)
            }
            None => None,
        };

        let query_input =
            get_transform_query_input(input_decl, input_state, maybe_input_head, datasets_map)
                .await?;

        get_transform_input_from_query_input(query_input, input_alias, None, datasets_map)
            .await
            .map_err(Into::into)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TransformElaborationService for TransformElaborationServiceImpl {
    #[tracing::instrument(level = "info", skip_all, fields(target=%plan.preliminary_request.dataset_handle))]
    async fn elaborate_transform(
        &self,
        plan: TransformPreliminaryPlan,
    ) -> Result<TransformElaboration, TransformElaborateError> {
        match self
            .elaborate_preliminary_request(plan.preliminary_request, None, &plan.datasets_map)
            .await?
        {
            Some(request) => Ok(TransformElaboration::Elaborated(TransformPlan {
                request,
                datasets_map: plan.datasets_map,
            })),
            None => Ok(TransformElaboration::UpToDate),
        }
    }

    #[tracing::instrument(level = "info", skip_all, fields(target=%plan.preliminary_request.dataset_handle))]
    async fn rebuild_transform(
        &self,
        plan: TransformPreliminaryPlan,
        stored: &TransformPlanPayload,
    ) -> Result<TransformPlan, TransformRebuildError> {
        let dataset_id = plan.preliminary_request.dataset_handle.id.clone();
        let mismatch = |reason: &str| TransformPlanMismatchError {
            dataset_id: dataset_id.clone(),
            reason: reason.to_string(),
        };

        if plan.preliminary_request.head != stored.expected_head {
            return Err(mismatch("the plan is not built at the stored expected head").into());
        }

        let pins = ElaborationPins {
            system_time: stored.system_time,
            input_heads: stored.input_heads(),
        };

        let request = match self
            .elaborate_preliminary_request(
                plan.preliminary_request,
                Some(&pins),
                &plan.datasets_map,
            )
            .await
        {
            Ok(Some(request)) => request,
            Ok(None) => return Err(mismatch("the rebuilt transform is up to date").into()),
            Err(TransformElaborateError::InputNotPulled(e)) => {
                return Err(mismatch(&e.to_string()).into());
            }
            Err(TransformElaborateError::InvalidInputInterval(e)) => {
                return Err(mismatch(&e.to_string()).into());
            }
            Err(TransformElaborateError::Internal(e)) => return Err(e.into()),
        };

        if TransformPlanPayload::from_request(&request) != *stored {
            tracing::warn!(
                ?stored,
                ?request,
                "Rebuilt transform differs from its stored plan"
            );
            return Err(mismatch("the rebuilt request does not map to the stored plan").into());
        }

        Ok(TransformPlan {
            request,
            datasets_map: plan.datasets_map,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
