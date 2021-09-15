use std::sync::Arc;

use opendatafabric as odf;
use slog::Logger;

use crate::{
    domain::*,
    infra::{utils::docker_client::DockerClient, WorkspaceLayout},
};

pub struct ODFEngine {
    container_runtime: DockerClient,
    image: String,
    workspace_layout: Arc<WorkspaceLayout>,
    logger: Logger,
}

impl ODFEngine {
    pub fn new(
        container_runtime: DockerClient,
        image: &str,
        workspace_layout: Arc<WorkspaceLayout>,
        logger: Logger,
    ) -> Self {
        Self {
            container_runtime,
            image: image.to_owned(),
            workspace_layout,
            logger,
        }
    }

    fn transform2(
        &self,
        _request: odf::ExecuteQueryRequest,
    ) -> Result<odf::ExecuteQueryResponseSuccess, EngineError> {
        todo!()
    }
}

impl Engine for ODFEngine {
    fn ingest(&self, _request: IngestRequest) -> Result<IngestResponse, EngineError> {
        unimplemented!()
    }

    fn transform(
        &self,
        mut request: ExecuteQueryRequest,
    ) -> Result<ExecuteQueryResponse, EngineError> {
        let mut inputs = Vec::new();
        for input_id in request.source.inputs {
            let slice = request.input_slices.remove(&input_id).unwrap();
            let vocab = request.dataset_vocabs.remove(&input_id).unwrap();
            inputs.push(odf::QueryInput {
                dataset_id: input_id,
                slice: odf::InputDataSlice {
                    interval: slice.interval,
                    schema_file: slice.schema_file.to_string_lossy().to_string(),
                    explicit_watermarks: slice.explicit_watermarks,
                },
                vocab: vocab,
            });
        }

        let request = odf::ExecuteQueryRequest {
            dataset_id: request.dataset_id,
            transform: request.source.transform,
            inputs: inputs,
        };

        let response = self.transform2(request)?;

        Ok(ExecuteQueryResponse {
            block: response.metadata_block,
        })
    }
}
