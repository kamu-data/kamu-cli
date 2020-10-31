use super::*;
use crate::domain::*;
use opendatafabric::serde::yaml::formats::datetime_rfc3339;
use opendatafabric::*;

use ::serde::{Deserialize, Serialize};
use ::serde_with::skip_serializing_none;
use chrono::{DateTime, Utc};
use std::fs::File;
use std::io::prelude::*;
use std::io::Error as IOError;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

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
    ) -> Result<ExecutionResult<PrepCheckpoint>, IngestError> {
        let mut stream: Box<dyn Stream> =
            Box::new(File::open(src_path).map_err(|e| IngestError::internal(e))?);

        for step in prep_steps.iter() {
            stream = match step {
                PrepStep::Pipe(ref p) => Box::new(
                    PipeStream::new(&p.command, stream).map_err(|e| IngestError::internal(e))?,
                ),
                PrepStep::Decompress(ref dc) => match dc.format {
                    CompressionFormat::Zip => Box::new(
                        DecompressZipStream::new(stream).map_err(|e| IngestError::internal(e))?,
                    ),
                    CompressionFormat::Gzip => Box::new(DecompressGzipStream::new(stream)),
                },
            };
        }

        let target_file = File::create(target_path).map_err(|e| IngestError::internal(e))?;
        let sink = Box::new(FileSink::new(target_file, stream));

        sink.join();

        Ok(ExecutionResult {
            was_up_to_date: false,
            checkpoint: PrepCheckpoint {
                last_prepared: Utc::now(),
                for_fetched_at: for_fetched_at,
            },
        })
    }
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

trait Stream: std::io::Read + Send {
    fn as_read(&mut self) -> &mut dyn std::io::Read;

    fn join(self: Box<Self>);
}

impl Stream for std::fs::File {
    fn as_read(&mut self) -> &mut dyn std::io::Read {
        self
    }

    fn join(self: Box<Self>) {}
}

///////////////////////////////////////////////////////////////////////////////
// Pipe Streams
///////////////////////////////////////////////////////////////////////////////

struct PipeStream {
    ingress: std::thread::JoinHandle<()>,
    stdout: std::process::ChildStdout,
}

impl PipeStream {
    fn new(cmd: &Vec<String>, mut input: Box<dyn Stream>) -> Result<Self, IOError> {
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

impl Read for PipeStream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, IOError> {
        self.stdout.read(buf)
    }
}

impl Stream for PipeStream {
    fn as_read(&mut self) -> &mut dyn std::io::Read {
        self
    }

    fn join(self: Box<Self>) {
        self.ingress.join().unwrap()
    }
}

///////////////////////////////////////////////////////////////////////////////
// DecompressZipStream
///////////////////////////////////////////////////////////////////////////////

struct ReaderHelper(Box<dyn Stream>);

impl Read for ReaderHelper {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, IOError> {
        self.0.read(buf)
    }
}

struct DecompressZipStream {
    ingress: std::thread::JoinHandle<()>,
    consumer: ringbuf::Consumer<u8>,
    done_flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl DecompressZipStream {
    fn new(input: Box<dyn Stream>) -> Result<Self, IOError> {
        let (mut producer, consumer) = ringbuf::RingBuffer::<u8>::new(BUFFER_SIZE).split();

        let done_flag = Arc::new(AtomicBool::new(false));
        let done_flag_thread = done_flag.clone();

        // TODO: Threading is a complete overkill here
        // Only reason for this is the `read_zipfile_from_stream` API that forces
        // use of mutable reference
        // See: https://github.com/mvdnes/zip-rs/issues/111
        let ingress = std::thread::Builder::new()
            .name("decompress_zip_stream".to_owned())
            .spawn(move || {
                let mut reader = ReaderHelper(input);

                {
                    let mut zipfile = zip::read::read_zipfile_from_stream(&mut reader)
                        .unwrap()
                        .unwrap();

                    loop {
                        let read = producer.read_from(&mut zipfile, None).unwrap();
                        if read == 0 {
                            break;
                        }
                    }
                }

                done_flag_thread.store(true, Ordering::Release);
                reader.0.join();
            })
            .unwrap();

        Ok(Self {
            ingress: ingress,
            consumer: consumer,
            done_flag: done_flag,
        })
    }
}

impl Read for DecompressZipStream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, IOError> {
        while self.consumer.is_empty() && !self.done_flag.load(Ordering::Acquire) {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        if !self.consumer.is_empty() {
            self.consumer.read(buf)
        } else {
            Ok(0)
        }
    }
}

impl Stream for DecompressZipStream {
    fn as_read(&mut self) -> &mut dyn std::io::Read {
        self
    }

    fn join(self: Box<Self>) {
        self.ingress.join().unwrap()
    }
}

///////////////////////////////////////////////////////////////////////////////
// DecompressGzipStream
///////////////////////////////////////////////////////////////////////////////

struct DecompressGzipStream {
    decoder: flate2::read::GzDecoder<Box<dyn Stream>>,
}

impl DecompressGzipStream {
    fn new(input: Box<dyn Stream>) -> Self {
        let decoder = flate2::read::GzDecoder::new(input);
        Self { decoder: decoder }
    }
}

impl Read for DecompressGzipStream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, IOError> {
        self.decoder.read(buf)
    }
}

impl Stream for DecompressGzipStream {
    fn as_read(&mut self) -> &mut dyn std::io::Read {
        self
    }

    fn join(self: Box<Self>) {
        self.decoder.into_inner().join()
    }
}

///////////////////////////////////////////////////////////////////////////////
// Sink
///////////////////////////////////////////////////////////////////////////////

struct FileSink {
    ingress: std::thread::JoinHandle<()>,
}

impl FileSink {
    fn new(mut file: std::fs::File, mut input: Box<dyn Stream>) -> Self {
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

    fn join(self: Box<Self>) {
        self.ingress.join().unwrap();
    }
}
