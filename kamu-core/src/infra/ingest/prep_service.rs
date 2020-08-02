use super::*;
use crate::domain::*;
use crate::infra::serde::yaml::formats::datetime_rfc3339;
use crate::infra::serde::yaml::*;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::fs::File;
use std::io::prelude::*;
use std::io::Error as IOError;
use std::path::Path;
use std::process::{Command, Stdio};

const BUFFER_SIZE: usize = 8096;

pub struct PrepService {}

impl PrepService {
    pub fn new() -> Self {
        Self {}
    }

    pub fn prepare(
        &self,
        prep_steps: &Vec<PrepStep>,
        for_fetched_at: DateTime<Utc>,
        _old_checkpoint: Option<PrepCheckpoint>,
        src_path: &Path,
        target_path: &Path,
    ) -> Result<ExecutionResult<PrepCheckpoint>, PrepError> {
        let step = prep_steps.get(0).unwrap();

        let src_file = File::open(src_path).map_err(|e| PrepError::internal(e))?;

        let stream = match step {
            PrepStep::Pipe(ref p) => self.pipe(p, src_file).map_err(|e| PrepError::internal(e))?,
            _ => unimplemented!(),
        };

        let target_file = File::create(target_path).map_err(|e| PrepError::internal(e))?;
        let sink = FileSink::new(target_file, stream);

        sink.join();

        Ok(ExecutionResult {
            was_up_to_date: false,
            checkpoint: PrepCheckpoint {
                last_prepared: Utc::now(),
                for_fetched_at: for_fetched_at,
            },
        })
    }

    fn pipe(
        &self,
        step: &PrepStepPipe,
        src: impl Stream + 'static,
    ) -> Result<PipeStream, std::io::Error> {
        PipeStream::new(&step.command, src)
    }
}

#[derive(Debug)]
pub struct PrepResult {
    was_updated: bool,
    checkpoint: PrepCheckpoint,
}

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrepCheckpoint {
    #[serde(with = "datetime_rfc3339")]
    pub last_prepared: DateTime<Utc>,
    #[serde(with = "datetime_rfc3339")]
    pub for_fetched_at: DateTime<Utc>,
}

///////////////////////////////////////////////////////////////////////////////
// Ghetto Streams
///////////////////////////////////////////////////////////////////////////////

// TODO: Error handling (?)

trait Stream: Send {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, IOError>;
    fn join(self);
}

impl Stream for std::fs::File {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, IOError> {
        <std::fs::File as std::io::Read>::read(self, buf)
    }

    fn join(self) {}
}

struct PipeStream {
    ingress: std::thread::JoinHandle<()>,
    stdout: std::process::ChildStdout,
}

impl PipeStream {
    fn new(cmd: &Vec<String>, mut input: impl Stream + 'static) -> Result<Self, IOError> {
        let process = Command::new(cmd.get(0).unwrap())
            .args(&cmd[1..])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;

        let mut stdin = process.stdin.unwrap();
        let ingress = std::thread::Builder::new()
            .name("pipe_stream".to_owned())
            .spawn(move || {
                let mut buf = [0; BUFFER_SIZE];

                loop {
                    let read = input.read(&mut buf).unwrap();
                    if read == 0 {
                        break;
                    }
                    stdin.write_all(&buf[..read]).unwrap();
                }

                input.join();
            })
            .unwrap();

        Ok(Self {
            ingress: ingress,
            stdout: process.stdout.unwrap(),
        })
    }
}

impl Stream for PipeStream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, IOError> {
        self.stdout.read(buf)
    }

    fn join(self) {
        self.ingress.join().unwrap()
    }
}

struct FileSink {
    ingress: std::thread::JoinHandle<()>,
}

impl FileSink {
    fn new(mut file: std::fs::File, mut input: impl Stream + 'static) -> Self {
        let ingress = std::thread::Builder::new()
            .name("file_sink".to_owned())
            .spawn(move || {
                let mut buf = [0; BUFFER_SIZE];

                loop {
                    let read = input.read(&mut buf).unwrap();
                    if read == 0 {
                        break;
                    }
                    file.write_all(&buf[..read]).unwrap();
                }

                input.join();
            })
            .unwrap();

        Self { ingress: ingress }
    }

    fn join(self) {
        self.ingress.join().unwrap();
    }
}
