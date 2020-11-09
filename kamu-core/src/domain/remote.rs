use opendatafabric::{DatasetID, Sha3_256};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use url::Url;

use std::backtrace::Backtrace;
use std::path::{Path, PathBuf};
use thiserror::Error;

pub type RemoteID = str;
pub type RemoteIDBuf = String;

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Remote {
    pub url: Url,
}

pub trait RemoteClient {
    fn read_ref(&self, dataset_id: &DatasetID) -> Result<Option<Sha3_256>, RemoteError>;

    fn write(
        &mut self,
        dataset_id: &DatasetID,
        expected_head: Option<Sha3_256>,
        new_head: Sha3_256,
        blocks: &mut dyn Iterator<Item = (Sha3_256, Vec<u8>)>,
        data_files: &mut dyn Iterator<Item = &Path>,
        checkpoint_dir: &Path,
    ) -> Result<(), RemoteError>;

    fn read(
        &self,
        dataset_id: &DatasetID,
        expected_head: Sha3_256,
        last_seen_block: Option<Sha3_256>,
        tmp_dir: &Path,
    ) -> Result<RemoteReadResult, RemoteError>;
}

pub struct RemoteReadResult {
    pub blocks: Vec<Vec<u8>>,
    pub data_files: Vec<PathBuf>,
    pub checkpoint_dir: PathBuf,
}

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Error)]
pub enum RemoteError {
    #[error("Dataset does not exist")]
    DoesNotExist,
    #[error("Dataset diverged")]
    Diverged,
    #[error("Dataset was updated concurrently")]
    UpdatedConcurrently,
    #[error("IO error")]
    IOError {
        #[from]
        source: std::io::Error,
        #[backtrace]
        backtrace: Backtrace,
    },
    #[error("Protocol error")]
    ProtocolError(BoxedError),
}
