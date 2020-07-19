use super::*;
use crate::domain::*;

use std::backtrace::Backtrace;
use std::convert::TryFrom;

pub struct MetadataRepositoryFs {
    workspace_layout: WorkspaceLayout,
}

impl MetadataRepositoryFs {
    pub fn new(workspace_layout: WorkspaceLayout) -> MetadataRepositoryFs {
        MetadataRepositoryFs {
            workspace_layout: workspace_layout,
        }
    }
}

impl MetadataRepository for MetadataRepositoryFs {
    fn list_datasets(&self) -> Box<dyn Iterator<Item = DatasetIDBuf>> {
        let read_dir = std::fs::read_dir(&self.workspace_layout.datasets_dir).unwrap();
        Box::new(ListDatasetsIter { rd: read_dir })
    }

    fn get_metadata_chain(&self, dataset_id: &DatasetID) -> Result<Box<dyn MetadataChain>, Error> {
        let path = self.workspace_layout.datasets_dir.join(dataset_id.as_str());
        if !path.exists() {
            Err(Error::DoesNotExist {
                kind: ResourceKind::Dataset,
                id: (dataset_id as &str).to_owned(),
                backtrace: Backtrace::capture(),
            })
        } else {
            Ok(Box::new(MetadataChainFsYaml::new(path)))
        }
    }
}

struct ListDatasetsIter {
    rd: std::fs::ReadDir,
}
impl Iterator for ListDatasetsIter {
    type Item = DatasetIDBuf;
    fn next(&mut self) -> Option<Self::Item> {
        let res = self.rd.next()?;
        let path = res.unwrap();
        let name = path.file_name();
        Some(DatasetIDBuf::try_from(&name).unwrap())
    }
}
