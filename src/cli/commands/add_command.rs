use super::{Command, Error};
use kamu::domain::*;

pub struct AddCommand<'a> {
    dataset_service: &'a mut dyn DatasetService,
    snapshot_refs: Vec<String>,
}

impl AddCommand<'_> {
    pub fn new<'a, I>(dataset_service: &mut dyn DatasetService, snapshot_refs_iter: I) -> AddCommand
    where
        I: Iterator<Item = &'a str>,
    {
        AddCommand {
            dataset_service: dataset_service,
            snapshot_refs: snapshot_refs_iter.map(|s| s.to_owned()).collect(),
        }
    }
}

impl Command for AddCommand<'_> {
    fn run(&mut self) -> Result<(), Error> {
        let results: Vec<_> = self
            .snapshot_refs
            .iter()
            .map(|r| self.dataset_service.load_snapshot_from_ref(r))
            .collect();

        let snapshots: Vec<_> = results
            .into_iter()
            .zip(self.snapshot_refs.iter())
            .filter_map(|(res, sref)| match res {
                Ok(s) => Some(s),
                Err(e) => {
                    eprintln!(
                        "{}: {}\n  {}",
                        console::style("Failed to load data from").yellow(),
                        sref,
                        console::style(e).dim()
                    );
                    None
                }
            })
            .collect();

        if snapshots.len() != self.snapshot_refs.len() {
            return Err(Error::Aborted);
        }

        for (id, res) in self
            .dataset_service
            .create_datasets_from_snapshots(snapshots)
        {
            match res {
                Ok(_) => eprintln!("Added: {}", id),
                Err(err) => eprintln!("Failed: {} - {}", id, err),
            }
        }

        Ok(())
    }
}
