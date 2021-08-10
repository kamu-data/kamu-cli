use std::sync::Arc;

use kamu::domain::*;
use opendatafabric::DatasetID;

use super::{CLIError, Command};
use crate::output::OutputConfig;

///////////////////////////////////////////////////////////////////////////////
// Command
///////////////////////////////////////////////////////////////////////////////

pub struct VerifyCommand {
    transform_svc: Arc<dyn TransformService>,
    output_config: Arc<OutputConfig>,
    ids: Vec<String>,
    recursive: bool,
}

impl VerifyCommand {
    pub fn new<I, S>(
        transform_svc: Arc<dyn TransformService>,
        output_config: Arc<OutputConfig>,
        ids: I,
        recursive: bool,
    ) -> Self
    where
        I: Iterator<Item = S>,
        S: AsRef<str>,
    {
        Self {
            transform_svc,
            output_config,
            ids: ids.map(|s| s.as_ref().to_owned()).collect(),
            recursive,
        }
    }
}

impl Command for VerifyCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        let dataset_ids: Vec<_> = self
            .ids
            .iter()
            .map(|s| DatasetID::try_from(s).unwrap())
            .collect();

        if dataset_ids.len() != 1 {
            return Err(CLIError::usage_error(
                "Verifying multiple datasets at once is not yet supported",
            ));
        }

        if self.recursive {
            return Err(CLIError::usage_error(
                "Verifying datasets recursively is not yet supported",
            ));
        }

        self.transform_svc
            .verify(
                dataset_ids.iter().next().unwrap(),
                (None, None),
                VerificationOptions::default(),
                None,
            )
            .unwrap();

        Ok(())
    }
}
