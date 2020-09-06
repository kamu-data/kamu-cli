use super::{Command, Error};
use kamu::domain::*;

use chrono::DateTime;
use std::cell::RefCell;
use std::rc::Rc;

pub struct SetWatermarkCommand {
    pull_svc: Rc<RefCell<dyn PullService>>,
    ids: Vec<String>,
    all: bool,
    recursive: bool,
    watermark: String,
}

impl SetWatermarkCommand {
    pub fn new<I, S, S2>(
        pull_svc: Rc<RefCell<dyn PullService>>,
        ids: I,
        all: bool,
        recursive: bool,
        watermark: S2,
    ) -> Self
    where
        I: Iterator<Item = S>,
        S: AsRef<str>,
        S2: AsRef<str>,
    {
        Self {
            pull_svc: pull_svc,
            ids: ids.map(|s| s.as_ref().to_owned()).collect(),
            all: all,
            recursive: recursive,
            watermark: watermark.as_ref().to_owned(),
        }
    }
}

impl Command for SetWatermarkCommand {
    fn run(&mut self) -> Result<(), Error> {
        if self.ids.len() != 1 {
            return Err(Error::UsageError {
                msg: "Only one dataset can be provided when setting a watermark".to_owned(),
            });
        } else if self.recursive || self.all {
            return Err(Error::UsageError {
                msg: "Can't use all or recursive flags when setting a watermark".to_owned(),
            });
        }

        let watermark =
            DateTime::parse_from_rfc3339(&self.watermark).map_err(|_| Error::UsageError {
                msg: format!(
                    "Invalid timestamp {} should follow RFC3339 format, e.g. 2020-01-01T12:00:00Z",
                    self.watermark
                ),
            })?;

        let dataset_id = DatasetID::try_from(self.ids.get(0).unwrap()).unwrap();

        match self
            .pull_svc
            .borrow_mut()
            .set_watermark(dataset_id, watermark.into())
        {
            Ok(PullResult::Updated { block_hash }) => {
                eprintln!(
                    "{}",
                    console::style(format!("Committed new block {}", block_hash)).green()
                );
                Ok(())
            }
            Ok(_) => panic!("Unexpected result"),
            Err(e) => Err(DomainError::InfraError(e.into()).into()),
        }
    }
}
