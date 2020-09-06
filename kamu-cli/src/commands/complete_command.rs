use super::{Command, Error};
use kamu::domain::*;

use glob;
use std::cell::RefCell;
use std::fs;
use std::path;
use std::rc::Rc;

pub struct CompleteCommand {
    metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
    app: clap::App<'static, 'static>,
    input: String,
    current: usize,
}

// TODO: This is an extremely hacky way to implement the completion
// but we have to do this until clap supports custom completer functions
impl CompleteCommand {
    pub fn new(
        metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
        app: clap::App<'static, 'static>,
        input: String,
        current: usize,
    ) -> Self {
        Self {
            metadata_repo: metadata_repo,
            app: app,
            input: input,
            current: current,
        }
    }

    fn complete_dataset(&self, prefix: &str) {
        for dataset_id in self.metadata_repo.borrow().get_all_datasets() {
            if dataset_id.starts_with(prefix) {
                println!("{}", dataset_id);
            }
        }
    }

    fn complete_path(&self, prefix: &str) {
        let path = path::Path::new(prefix);
        let mut matched_dirs = 0;
        let mut last_matched_dir: path::PathBuf = path::PathBuf::new();

        if !path.exists() {
            let mut glb = path.to_str().unwrap().to_owned();
            glb.push('*');

            for entry in glob::glob(&glb).unwrap() {
                let p = entry.unwrap();
                if p.is_dir() {
                    println!("{}{}", p.display(), std::path::MAIN_SEPARATOR);
                    matched_dirs += 1;
                    last_matched_dir = p;
                } else {
                    println!("{}", p.display());
                }
            }
        } else if path.is_dir() {
            for entry in fs::read_dir(path).unwrap() {
                println!("{}", entry.unwrap().path().display());
            }
        }

        // HACK: to prevent a directory from fulfilling the completion fully
        // we add an extra result that should advance the completion
        // but not finish it.
        if matched_dirs == 1 && !last_matched_dir.to_str().unwrap().is_empty() {
            println!(
                "{}{}...",
                last_matched_dir.display(),
                std::path::MAIN_SEPARATOR
            );
        }
    }
}

impl Command for CompleteCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    fn run(&mut self) -> Result<(), Error> {
        let mut args = match shlex::split(&self.input) {
            Some(v) => v,
            _ => return Ok(()),
        };

        args.truncate(self.current + 1);

        let mut last_cmd = &self.app.p;

        // Establish command context
        for arg in args[1..].iter() {
            for s in last_cmd.subcommands.iter() {
                if s.p.meta.name == *arg {
                    last_cmd = &s.p;
                    continue;
                }
            }
        }

        let empty = "".to_owned();
        let to_complete = args.get(self.current).unwrap_or(&empty);

        // Complete commands
        for s in last_cmd.subcommands.iter() {
            if !s.p.is_set(clap::AppSettings::Hidden) && s.p.meta.name.starts_with(to_complete) {
                println!("{}", s.p.meta.name);
            }
        }

        // Complete positionals
        for pos in last_cmd.positionals.iter() {
            match pos.1.b.name {
                "dataset" => self.complete_dataset(to_complete),
                "snapshot" => self.complete_path(to_complete),
                _ => (),
            }
        }

        // Complete flags and options
        if to_complete.starts_with("-") {
            for flg in last_cmd.flags.iter() {
                let full_name = if flg.s.long.is_some() {
                    format!("--{}", flg.s.long.unwrap())
                } else {
                    format!("-{}", flg.s.short.unwrap())
                };
                if full_name.starts_with(to_complete) {
                    println!("{}", full_name);
                }
            }
            for opt in last_cmd.opts.iter() {
                let full_name = format!("--{}", opt.s.long.unwrap());
                if full_name.starts_with(to_complete) {
                    println!("{}", full_name);
                }
            }
        }

        Ok(())
    }
}
