// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ffi::{OsStr, OsString};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use kamu::domain::MetadataRepository;
use kamu::infra::*;
use kamu_cli::CLIError;
use opendatafabric::serde::yaml::*;
use opendatafabric::*;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use thiserror::Error;

// Test wrapper on top of CLI library
pub struct Kamu {
    workspace_layout: WorkspaceLayout,
    volume_layout: VolumeLayout,
    workspace_path: PathBuf,
    _temp_dir: Option<tempfile::TempDir>,
}

impl Kamu {
    pub fn new(workspace_path: &Path) -> Self {
        let workspace_layout = WorkspaceLayout::new(workspace_path);
        let volume_layout = VolumeLayout::new(&workspace_layout.local_volume_dir);
        Self {
            workspace_layout,
            volume_layout,
            workspace_path: workspace_path.to_owned(),
            _temp_dir: None,
        }
    }

    pub fn new_workspace_tmp() -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let inst = Self::new(temp_dir.path());
        let inst = Self {
            _temp_dir: Some(temp_dir),
            ..inst
        };

        inst.execute(["init"]).unwrap();

        // TODO: Remove when podman is the default
        inst.execute(["config", "set", "engine.runtime", "podman"])
            .unwrap();

        inst
    }

    pub fn workspace_path(&self) -> &Path {
        &self.workspace_path
    }

    //pub fn volume_layout(&self) -> &VolumeLayout {
    //    &self.volume_layout
    //}

    pub fn dataset_layout(&self, dataset_name: &DatasetName) -> DatasetLayout {
        DatasetLayout::new(&self.volume_layout, dataset_name)
    }

    pub fn get_last_data_slice(&self, dataset_name: &DatasetName) -> ParquetHelper {
        let metadata_repo = MetadataRepositoryImpl::new(Arc::new(self.workspace_layout.clone()));

        let part_file = metadata_repo
            .get_metadata_chain(&dataset_name.as_local_ref())
            .unwrap()
            .iter_blocks()
            .map(|(h, _)| {
                self.dataset_layout(dataset_name)
                    .data_dir
                    .join(h.to_string())
            })
            .filter(|p| p.is_file())
            .next()
            .expect("Data file not found");

        ParquetHelper::open(&part_file)
    }

    pub fn execute<I, S>(&self, cmd: I) -> Result<(), CommandError>
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        let mut full_cmd = vec![OsStr::new("kamu").to_owned(), OsStr::new("-q").to_owned()];
        full_cmd.extend(cmd.into_iter().map(|i| i.into()));

        let app = kamu_cli::cli();
        let matches = app.get_matches_from_safe(&full_cmd).unwrap();

        kamu_cli::run(
            self.workspace_layout.clone(),
            self.volume_layout.clone(),
            matches,
        )
        .map_err(|e| CommandError {
            cmd: full_cmd,
            error: e,
        })
    }

    pub fn add_dataset(&self, dataset_snapshot: DatasetSnapshot) -> Result<(), CommandError> {
        let content = YamlDatasetSnapshotSerializer
            .write_manifest(&dataset_snapshot)
            .unwrap();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.as_file().write(&content).unwrap();
        f.flush().unwrap();

        self.execute(["add".as_ref(), f.path().as_os_str()])
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Command {cmd:?} failed: {error}")]
pub struct CommandError {
    cmd: Vec<OsString>,
    error: CLIError,
}

/////////////////////////////////////////////////////////////////////////////////////////

pub struct ParquetHelper {
    pub reader: SerializedFileReader<std::fs::File>,
}

impl ParquetHelper {
    fn new(reader: SerializedFileReader<std::fs::File>) -> Self {
        Self { reader }
    }

    fn open(path: &Path) -> Self {
        Self::new(SerializedFileReader::new(std::fs::File::open(&path).unwrap()).unwrap())
    }

    pub fn column_names(&self) -> Vec<String> {
        self.reader
            .metadata()
            .file_metadata()
            .schema_descr()
            .columns()
            .iter()
            .map(|cd| cd.path().string())
            .collect()
    }

    pub fn records<F, R>(&self, row_mapper: F) -> Vec<R>
    where
        R: Ord,
        F: Fn(parquet::record::Row) -> R,
    {
        let mut records: Vec<_> = self
            .reader
            .get_row_iter(None)
            .unwrap()
            .map(row_mapper)
            .collect();

        records.sort();
        records
    }
}
