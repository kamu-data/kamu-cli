use super::fetch::*;
use crate::domain::*;
use crate::infra::serde::yaml::*;
use crate::infra::*;

pub struct IngestTask<'a> {
    dataset_id: DatasetIDBuf,
    layout: DatasetLayout,
    source: DatasetSourceRoot,
    meta_chain: &'a mut dyn MetadataChain,
    listener: &'a mut dyn IngestListener,
}

impl IngestTask<'_> {
    pub fn new<'a>(
        dataset_id: &DatasetID,
        layout: DatasetLayout,
        meta_chain: &'a mut dyn MetadataChain,
        listener: &'a mut dyn IngestListener,
    ) -> IngestTask<'a> {
        // TODO: this is expensive
        let source = match meta_chain.iter_blocks().filter_map(|b| b.source).next() {
            Some(DatasetSource::Root(src)) => src,
            _ => panic!("Failed to find source definition"),
        };

        IngestTask {
            dataset_id: dataset_id.to_owned(),
            layout: layout,
            source: source,
            meta_chain: meta_chain,
            listener: listener,
        }
    }

    // Note: Can be called from multiple threads
    pub fn ingest(&mut self) -> Result<IngestResult, IngestError> {
        self.listener.begin();

        self.listener
            .on_stage_progress(IngestStage::CheckCache, 0, 1);

        self.maybe_fetch()?;
        Ok(IngestResult::UpToDate)
    }

    fn maybe_fetch(&mut self) -> Result<FetchResult, FetchError> {
        let fetch_svc = FetchService::new();
        let checkpoint_path = self.layout.cache_dir.join("fetch.yaml");
        let target_path = self.layout.cache_dir.join("fetched.bin");
        let mut listener = FetchProgressListenerBridge {
            listener: self.listener,
        };
        fetch_svc.fetch(
            &self.source.fetch,
            &checkpoint_path,
            &target_path,
            Some(&mut listener),
        )
    }
}

struct FetchProgressListenerBridge<'a> {
    listener: &'a mut dyn IngestListener,
}

impl FetchProgressListener for FetchProgressListenerBridge<'_> {
    fn on_progress(&mut self, progress: &FetchProgress) {
        self.listener.on_stage_progress(
            IngestStage::Fetch,
            progress.fetched_bytes,
            progress.total_bytes,
        );
    }
}
