use super::{Command, Error};
use crate::output::OutputFormat;
use kamu::infra::explore::*;
use kamu::infra::*;

use console::style as s;
use indoc::indoc;

pub struct NotebookCommand {
    workspace_layout: WorkspaceLayout,
    volume_layout: VolumeLayout,
    output_format: OutputFormat,
    env_vars: Vec<(String, Option<String>)>,
}

impl NotebookCommand {
    pub fn new<Iter, Str>(
        workspace_layout: &WorkspaceLayout,
        volume_layout: &VolumeLayout,
        output_format: &OutputFormat,
        env_vars: Iter,
    ) -> Self
    where
        Iter: IntoIterator<Item = Str>,
        Str: AsRef<str>,
    {
        Self {
            workspace_layout: workspace_layout.clone(),
            volume_layout: volume_layout.clone(),
            output_format: output_format.clone(),
            env_vars: env_vars
                .into_iter()
                .map(|elem| {
                    let s = elem.as_ref();
                    match s.find("=") {
                        None => (s.to_owned(), None),
                        Some(pos) => {
                            let (name, value) = s.split_at(pos);
                            (name.to_owned(), Some(value[1..].to_owned()))
                        }
                    }
                })
                .collect(),
        }
    }
}

impl Command for NotebookCommand {
    fn run(&mut self) -> Result<(), Error> {
        let environment_vars = self
            .env_vars
            .iter()
            .map(|(name, value)| {
                value
                    .clone()
                    .or_else(|| std::env::var(name).ok())
                    .ok_or_else(|| Error::UsageError {
                        msg: format!("Environment variable {} is not set", name),
                    })
                    .map(|v| (name.to_owned(), v))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let spinner = if self.output_format.verbosity_level == 0 {
            let s = indicatif::ProgressBar::new_spinner();
            s.set_style(
                indicatif::ProgressStyle::default_spinner().template("{spinner:.cyan} {msg}"),
            );
            s.set_message("Starting Jupyter server");
            s.enable_steady_tick(100);
            Some(s)
        } else {
            None
        };

        NotebookServerImpl::run(
            &self.workspace_layout,
            &self.volume_layout,
            environment_vars,
            self.output_format.verbosity_level > 0,
            move |url| {
                if let Some(s) = spinner {
                    s.finish_and_clear()
                }
                eprintln!(
                    "{}\n  {}",
                    s("Jupyter server is now running at:").green().bold(),
                    s(url).bold(),
                );
                eprintln!(
                    "{}",
                    s(indoc!(
                        "Note: On some platforms that run Docker in the virtual machine you may
                        need to substitute `localhost` with the VM's address (e.g. `boot2docker`)."
                    ))
                    .dim()
                );
                eprintln!("{}", s("Use Ctrl+C to stop the server").yellow());
                let _ = webbrowser::open(url);
            },
            || eprintln!("{}", s("Shutting down").yellow()),
        )?;
        Ok(())
    }
}
