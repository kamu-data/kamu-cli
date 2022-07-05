use crate::domain::*;
use std::sync::Arc;
use dill::*;
use opendatafabric::*;


pub struct ResetServiceImpl {
    local_repo: Arc<dyn LocalDatasetRepository>
}

#[component(pub)]
impl ResetServiceImpl {
    pub fn new(
        local_repo: Arc<dyn LocalDatasetRepository>,
    ) -> Self {
        Self {
            local_repo,
        }
    }
}


#[async_trait::async_trait(?Send)]
impl ResetService for ResetServiceImpl {
    async fn reset_dataset(
        &self,
        dataset_ref: &DatasetRefLocal,
        block_hash: &Multihash,
    ) -> Result<(), ResetError> {

        let dataset_handle = self.local_repo.resolve_dataset_ref(dataset_ref).await?;
        let dataset = self
            .local_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await?;

        dataset
            .as_metadata_chain()
            .set_ref(
                &BlockRef::Head, 
                block_hash, 
                SetRefOpts {
                    validate_block_present: true,
                    check_ref_is: Option::None,
                },
            )
            .await?;

        Ok(())
    }    
}
