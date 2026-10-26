// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use engine::{TransformRequestExt, TransformRequestInputExt};
use kamu_core::*;
use kamu_datasets::ResolvedDatasetsMap;
use random_strings::get_random_name;
use time_source::SystemTimeSource;

use super::get_transform_input_from_query_input;
use crate::get_transform_query_input;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TransformElaborationServiceImpl {
    time_source: Arc<dyn SystemTimeSource>,
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
        datasets_map: &ResolvedDatasetsMap,
    ) -> Result<Option<TransformRequestExt>, TransformElaborateError> {
        use futures::{StreamExt, TryStreamExt};
        let inputs: Vec<_> = futures::stream::iter(preliminary_request.input_states)
            .then(|(input_decl, input_state)| {
                self.get_transform_input(input_decl, input_state, datasets_map)
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
            operation_id: get_random_name(None, 10),
            dataset_handle: preliminary_request.dataset_handle,
            block_ref: preliminary_request.block_ref,
            head: preliminary_request.head,
            transform: preliminary_request.transform,
            system_time: self.time_source.now(),
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
        datasets_map: &ResolvedDatasetsMap,
    ) -> Result<TransformRequestInputExt, TransformElaborateError> {
        let input_alias = input_decl.alias.clone().unwrap();

        let query_input = get_transform_query_input(input_decl, input_state, datasets_map).await?;

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
            .elaborate_preliminary_request(plan.preliminary_request, &plan.datasets_map)
            .await?
        {
            Some(request) => Ok(TransformElaboration::Elaborated(TransformPlan {
                request,
                datasets_map: plan.datasets_map,
            })),
            None => Ok(TransformElaboration::UpToDate),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
