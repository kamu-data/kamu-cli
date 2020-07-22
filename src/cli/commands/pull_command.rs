use super::{Command, Error};
use indicatif::*;
use kamu::domain::*;

pub struct PullCommand<'a> {
    metadata_repo: &'a dyn MetadataRepository,
    ingest_svc: &'a mut dyn IngestService,
    ids: Vec<String>,
    all: bool,
    recursive: bool,
}

impl PullCommand<'_> {
    pub fn new<'a, 's, I>(
        metadata_repo: &'a dyn MetadataRepository,
        ingest_svc: &'a mut dyn IngestService,
        ids: I,
        all: bool,
        recursive: bool,
    ) -> PullCommand<'a>
    where
        I: Iterator<Item = &'s str>,
    {
        PullCommand {
            metadata_repo: metadata_repo,
            ingest_svc: ingest_svc,
            ids: ids.map(|s| s.into()).collect(),
            all: all,
            recursive: recursive,
        }
    }
}

impl Command for PullCommand<'_> {
    fn run(&mut self) -> Result<(), Error> {
        /*let bar = ProgressBar::new(100);
        bar.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {wide_bar:.cyan/blue} {pos:>5}/{len:>5} {msg}"),
        );
        for _ in 0..100 {
            bar.inc(1);
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        bar.finish_with_message("OK");*/
        Ok(())
    }
}
