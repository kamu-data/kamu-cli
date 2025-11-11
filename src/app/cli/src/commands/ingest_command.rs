// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use file_utils::MediaType;
use kamu::domain::*;

use super::{CLIError, Command};
use crate::OutputConfig;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Command
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct IngestCommand {
    output_config: Arc<OutputConfig>,
    data_format_reg: Arc<dyn DataFormatRegistry>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    push_ingest_data_use_case: Arc<dyn PushIngestDataUseCase>,

    #[dill::component(explicit)]
    dataset_ref: odf::DatasetRef,

    #[dill::component(explicit)]
    files_refs: Vec<String>,

    #[dill::component(explicit)]
    source_name: Option<String>,

    #[dill::component(explicit)]
    event_time: Option<String>,

    #[dill::component(explicit)]
    stdin: bool,

    #[dill::component(explicit)]
    recursive: bool,

    #[dill::component(explicit)]
    input_format: Option<String>,
}

impl IngestCommand {
    fn path_to_url(path: &str) -> Result<url::Url, CLIError> {
        let p = PathBuf::from(path)
            .canonicalize()
            .map_err(|e| CLIError::usage_error(format!("Invalid path {path}: {e}")))?;
        url::Url::from_file_path(p)
            .map_err(|_| CLIError::usage_error(format!("Invalid path {path}")))
    }

    async fn ensure_valid_push_target(
        &self,
        dataset_handle: &odf::DatasetHandle,
    ) -> Result<ResolvedDataset, CLIError> {
        let aliases = self
            .remote_alias_reg
            .get_remote_aliases(dataset_handle)
            .await
            .map_err(CLIError::failure)?;
        let pull_aliases: Vec<_> = aliases
            .get_by_kind(RemoteAliasKind::Pull)
            .map(ToString::to_string)
            .collect();
        if !pull_aliases.is_empty() {
            return Err(CLIError::usage_error(format!(
                "Ingesting data into remote dataset will cause histories to diverge. Existing \
                 pull aliases:\n{}",
                pull_aliases.join("\n- ")
            )));
        }

        let resolved_dataset = self
            .dataset_registry
            .get_dataset_by_handle(dataset_handle)
            .await;

        if resolved_dataset.get_kind() != odf::DatasetKind::Root {
            return Err(CLIError::usage_error(
                "Ingesting data available for root datasets only",
            ));
        }
        Ok(resolved_dataset)
    }

    fn get_media_type(&self) -> Result<Option<MediaType>, CLIError> {
        let Some(short_name) = &self.input_format else {
            return Ok(None);
        };

        let short_name_lower = short_name.to_lowercase();

        for fmt in self.data_format_reg.list_formats() {
            if fmt.short_name.to_lowercase() == short_name_lower {
                return Ok(Some(fmt.media_type.to_owned()));
            }
        }

        Err(CLIError::usage_error(format!(
            "Unsupported format {short_name}"
        )))
    }
}

#[async_trait::async_trait(?Send)]
impl Command for IngestCommand {
    async fn run(&self) -> Result<(), CLIError> {
        match (self.stdin, !self.files_refs.is_empty()) {
            (false, false) | (true, true) => Err(CLIError::usage_error(
                "Specify a list of files or pass --stdin",
            )),
            _ => Ok(()),
        }?;

        // TODO: `kamu ingest`: implement `--recursive` mode
        //        https://github.com/kamu-data/kamu-cli/issues/886
        if self.recursive {
            unimplemented!("Sorry, recursive ingest is not yet implemented")
        }

        let dataset_handle = self
            .dataset_registry
            .resolve_dataset_handle_by_ref(&self.dataset_ref)
            .await
            .map_err(CLIError::failure)?;

        let target_dataset = self.ensure_valid_push_target(&dataset_handle).await?;

        let urls = if self.stdin {
            vec![url::Url::parse("file:///dev/fd/0").unwrap()]
        } else {
            self.files_refs
                .iter()
                .map(|path| Self::path_to_url(path))
                .collect::<Result<Vec<_>, _>>()?
        };

        let listener = if !self.output_config.is_tty
            || self.output_config.quiet
            || self.output_config.verbosity_level != 0
            || self.stdin
        {
            None
        } else {
            Some(Arc::new(PushIngestProgress::new(
                &dataset_handle,
                Arc::new(indicatif::MultiProgress::new()),
            )) as Arc<dyn PushIngestListener>)
        };

        let source_event_time: Option<DateTime<Utc>> = self
            .event_time
            .as_ref()
            .map(|s| DateTime::parse_from_rfc3339(s))
            .transpose()
            .map_err(CLIError::usage_error_from)?
            .map(Into::into);

        let mut updated = 0;
        for url in urls {
            let ingest_result = self
                .push_ingest_data_use_case
                .execute(
                    target_dataset.clone(),
                    DataSource::Url(url),
                    PushIngestDataUseCaseOptions {
                        source_name: self.source_name.clone(),
                        source_event_time,
                        is_ingest_from_upload: false,
                        media_type: self.get_media_type()?,
                        expected_head: None,
                    },
                    listener.clone(),
                )
                .await
                .map_err(CLIError::failure)?;

            match ingest_result {
                PushIngestResult::UpToDate => (),
                PushIngestResult::Updated { .. } => updated += 1,
            }
        }

        if !self.output_config.quiet {
            if updated != 0 {
                eprintln!("{}", console::style("Dataset updated").green().bold());
            } else {
                eprintln!("{}", console::style("Dataset up-to-date").yellow().bold());
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct PushIngestProgress {
    dataset_handle: odf::DatasetHandle,
    multi_progress: Arc<indicatif::MultiProgress>,
    state: Mutex<PushIngestProgressState>,
}

struct PushIngestProgressState {
    stage: PushIngestStage,
    curr_progress: indicatif::ProgressBar,
}

impl PushIngestProgress {
    pub fn new(
        dataset_handle: &odf::DatasetHandle,
        multi_progress: Arc<indicatif::MultiProgress>,
    ) -> Self {
        Self {
            state: Mutex::new(PushIngestProgressState {
                stage: PushIngestStage::Read,
                curr_progress: multi_progress.add(Self::new_spinner(&Self::spinner_message(
                    dataset_handle,
                    0,
                    "Reading data",
                ))),
            }),
            dataset_handle: dataset_handle.clone(),
            multi_progress,
        }
    }

    fn new_spinner(msg: &str) -> indicatif::ProgressBar {
        let pb = indicatif::ProgressBar::hidden();
        let style = indicatif::ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .unwrap();
        pb.set_style(style);
        pb.set_message(msg.to_owned());
        pb.enable_steady_tick(Duration::from_millis(100));
        pb
    }

    fn spinner_message<T: std::fmt::Display>(
        dataset_handle: &odf::DatasetHandle,
        step: u32,
        msg: T,
    ) -> String {
        let step_str = format!("[{}/7]", step + 1);
        let dataset = format!("({})", dataset_handle.alias);
        format!(
            "{} {} {}",
            console::style(step_str).bold().dim(),
            msg,
            console::style(dataset).dim(),
        )
    }

    fn message_for_stage(&self, stage: PushIngestStage) -> String {
        let msg = match stage {
            PushIngestStage::Read => "Reading data",
            PushIngestStage::Preprocess => "Preprocessing data",
            PushIngestStage::Merge => "Merging data",
            PushIngestStage::Commit => "Committing data",
        };
        Self::spinner_message(&self.dataset_handle, stage as u32, msg)
    }
}

impl PushIngestListener for PushIngestProgress {
    fn on_stage_progress(&self, stage: PushIngestStage, _n: u64, _out_of: TotalSteps) {
        let mut state = self.state.lock().unwrap();
        state.stage = stage;

        if state.curr_progress.is_finished() {
            state.curr_progress.finish();
            state.curr_progress = self
                .multi_progress
                .add(Self::new_spinner(&self.message_for_stage(stage)));
        } else {
            state
                .curr_progress
                .set_message(self.message_for_stage(stage));
        }
    }

    fn success(&self, result: &PushIngestResult) {
        let state = self.state.lock().unwrap();

        match result {
            PushIngestResult::UpToDate => {
                state
                    .curr_progress
                    .finish_with_message(Self::spinner_message(
                        &self.dataset_handle,
                        PushIngestStage::Commit as u32,
                        console::style("Dataset is up-to-date".to_owned()).yellow(),
                    ));
            }
            PushIngestResult::Updated { new_head, .. } => {
                state
                    .curr_progress
                    .finish_with_message(Self::spinner_message(
                        &self.dataset_handle,
                        PollingIngestStage::Commit as u32,
                        console::style(format!(
                            "Committed new block {}",
                            new_head.as_multibase().short()
                        ))
                        .green(),
                    ));
            }
        }
    }

    fn error(&self, _error: &PushIngestError) {
        let state = self.state.lock().unwrap();
        state
            .curr_progress
            .finish_with_message(Self::spinner_message(
                &self.dataset_handle,
                state.stage as u32,
                console::style("Failed to update root dataset").red(),
            ));
    }

    fn get_pull_image_listener(self: Arc<Self>) -> Option<Arc<dyn PullImageListener>> {
        Some(self)
    }

    fn get_engine_provisioning_listener(
        self: Arc<Self>,
    ) -> Option<Arc<dyn EngineProvisioningListener>> {
        Some(self)
    }
}

impl EngineProvisioningListener for PushIngestProgress {
    fn begin(&self, engine_id: &str) {
        let state = self.state.lock().unwrap();

        // This currently happens during the Read stage
        state.curr_progress.set_message(Self::spinner_message(
            &self.dataset_handle,
            PollingIngestStage::Read as u32,
            format!("Waiting for engine {engine_id}"),
        ));
    }

    fn success(&self) {
        self.on_stage_progress(PushIngestStage::Read, 0, TotalSteps::Exact(0));
    }

    fn get_pull_image_listener(self: Arc<Self>) -> Option<Arc<dyn PullImageListener>> {
        Some(self)
    }
}

impl PullImageListener for PushIngestProgress {
    fn begin(&self, image: &str) {
        let state = self.state.lock().unwrap();

        // This currently happens during the Read stage
        state.curr_progress.set_message(Self::spinner_message(
            &self.dataset_handle,
            PollingIngestStage::Read as u32,
            format!("Pulling image {image}"),
        ));
    }

    fn success(&self) {
        // Careful not to deadlock
        let stage = {
            let state = self.state.lock().unwrap();
            state.curr_progress.finish();
            state.stage
        };
        self.on_stage_progress(stage, 0, TotalSteps::Exact(0));
    }
}
