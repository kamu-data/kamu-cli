// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::domain::utils::metadata_chain_comparator::CompareChainsResult;
use kamu::domain::{PushStatus, RemoteStatusService};

use crate::{CLIError, Interact};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ConfirmDeleteService {
    push_status_service: Arc<dyn RemoteStatusService>,
    interact: Arc<Interact>,
}

#[dill::component(pub)]
impl ConfirmDeleteService {
    pub fn new(push_status_service: Arc<dyn RemoteStatusService>, interact: Arc<Interact>) -> Self {
        Self {
            push_status_service,
            interact,
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?dataset_handles))]
    pub async fn confirm_delete(
        &self,
        dataset_handles: &[odf::DatasetHandle],
    ) -> Result<(), CLIError> {
        for hdl in dataset_handles {
            let statuses = self.push_status_service.check_remotes_status(hdl).await?;

            let mut out_of_sync = vec![];
            let mut unknown = vec![];

            for PushStatus {
                remote,
                check_result,
            } in &statuses.statuses
            {
                match check_result {
                    Ok(status) => {
                        let maybe_status_desc = match status {
                            CompareChainsResult::LhsAhead { .. } => Some("ahead of"),
                            CompareChainsResult::LhsBehind { .. } => Some("behind"),
                            CompareChainsResult::Divergence { .. } => Some("diverged from"),
                            _ => None,
                        };
                        if let Some(status_desc) = maybe_status_desc {
                            out_of_sync.push((remote, status_desc));
                        }
                    }
                    Err(e) => unknown.push((remote, e)),
                }
            }

            let all_synced = out_of_sync.is_empty() && unknown.is_empty();
            tracing::debug!(%all_synced, ?out_of_sync, ?unknown, "Checking remote status finished");

            if !all_synced {
                eprintln!(
                    "{} dataset is out of sync with remote(s):",
                    console::style(&hdl.alias).bold()
                );
                for (remote, status_desc) in out_of_sync {
                    eprintln!(
                        " - {} '{}'",
                        console::style(status_desc).yellow(),
                        console::style(&remote).bold(),
                    );
                }
                for (remote, err) in unknown {
                    eprintln!(
                        " - {} '{}'. Error: {}",
                        console::style("could not check state of").yellow(),
                        console::style(&remote).bold(),
                        err,
                    );
                }
                eprintln!();
            };
        }

        self.interact.require_confirmation(format!(
            "{}\n  {}\n{}",
            console::style("You are about to delete following dataset(s):").yellow(),
            itertools::join(dataset_handles.iter().map(|h| &h.alias), "\n  "),
            console::style("This operation is irreversible!").yellow(),
        ))?;
        Ok(())
    }
}
