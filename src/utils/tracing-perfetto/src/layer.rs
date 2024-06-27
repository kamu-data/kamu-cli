// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use conv::ConvUtil;
use tracing::*;
use tracing_subscriber::*;

use crate::json_writer::*;
use crate::perfetto_types::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

thread_local! {
    static TID: RefCell<Option<u64>> = const { RefCell::new(None) };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PerfettoLayer {
    start: Instant,
    writer: Arc<Mutex<PerfettoWriterJson<std::fs::File>>>,
    next_thread_id: AtomicU64,
    next_span_id: AtomicU64,
}

impl PerfettoLayer {
    pub fn new(path: impl Into<std::path::PathBuf>) -> (Self, FlushGuard) {
        let writer = Arc::new(Mutex::new(PerfettoWriterJson::new(
            std::fs::File::create(path.into()).unwrap(),
        )));

        (
            Self {
                start: Instant::now(),
                writer: writer.clone(),
                next_thread_id: AtomicU64::new(1),
                next_span_id: AtomicU64::new(1),
            },
            FlushGuard {
                writer: Some(writer),
            },
        )
    }

    fn get_ts(&self) -> f64 {
        let nanos = u64::try_from(self.start.elapsed().as_nanos()).unwrap();

        nanos.value_as::<f64>().unwrap() / 1000.0
    }

    fn get_tid(&self) -> u64 {
        let (tid, new) = TID.with(|value| {
            let tid = *value.borrow();
            match tid {
                Some(tid) => (tid, false),
                None => {
                    let tid = self.next_thread_id.fetch_add(1, Ordering::SeqCst);
                    value.replace(Some(tid));
                    (tid, true)
                }
            }
        });

        if new {
            let name = format!(
                "Thread: {}",
                std::thread::current().name().unwrap_or("<unnamed>")
            );
            self.writer
                .lock()
                .unwrap()
                .write_metadata(&PerfettoMetadata::ThreadName { tid, name: &name });
        }

        tid
    }

    fn write_event(&self, event: PerfettoEvent<'_>) {
        self.writer.lock().unwrap().write_event(event);
    }
}

impl<S> Layer<S> for PerfettoLayer
where
    S: Subscriber + for<'span> registry::LookupSpan<'span> + Send + Sync,
{
    fn on_enter(&self, id: &span::Id, ctx: layer::Context<'_, S>) {
        let span: registry::SpanRef<'_, S> = ctx.span(id).unwrap();
        self.write_event(PerfettoEvent {
            ts: self.get_ts(),
            pid: 1,
            tid: self.get_tid(),
            phase: Phase::Begin,
            name: Some(span.metadata().name()),
            id: None,
            args: None,
            scope: None,
        });
    }

    fn on_exit(&self, _id: &span::Id, _ctx: layer::Context<'_, S>) {
        self.write_event(PerfettoEvent {
            ts: self.get_ts(),
            pid: 1,
            tid: self.get_tid(),
            phase: Phase::End,
            name: None,
            id: None,
            args: None,
            scope: None,
        });
    }

    fn on_new_span(
        &self,
        attrs: &span::Attributes<'_>,
        id: &span::Id,
        _ctx: layer::Context<'_, S>,
    ) {
        let sid = self.next_span_id.fetch_add(1, Ordering::SeqCst);
        let name = format!("{}: {}", sid, attrs.metadata().name());

        self.write_event(PerfettoEvent {
            ts: self.get_ts(),
            pid: 1,
            tid: self.get_tid(),
            phase: Phase::BeginAsync,
            name: Some(&name),
            id: Some(id.into_u64()),
            args: Some(Visitable::Span(attrs)),
            scope: None,
        });
    }

    fn on_close(&self, id: span::Id, _ctx: layer::Context<'_, S>) {
        self.write_event(PerfettoEvent {
            ts: self.get_ts(),
            pid: 1,
            tid: self.get_tid(),
            phase: Phase::EndAsync,
            name: None,
            id: Some(id.into_u64()),
            args: None,
            scope: None,
        });
    }

    fn on_event(&self, event: &Event<'_>, ctx: layer::Context<'_, S>) {
        self.write_event(PerfettoEvent {
            ts: self.get_ts(),
            pid: 1,
            tid: self.get_tid(),
            phase: Phase::InstantAsync,
            name: Some(event.metadata().name()),
            id: ctx.current_span().id().map(Id::into_u64),
            args: Some(Visitable::Event(event)),
            scope: None,
        });
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlushGuard {
    writer: Option<Arc<Mutex<PerfettoWriterJson<std::fs::File>>>>,
}

impl Drop for FlushGuard {
    fn drop(&mut self) {
        if let Some(writer) = self.writer.take() {
            writer.lock().unwrap().finish();
        }
    }
}
