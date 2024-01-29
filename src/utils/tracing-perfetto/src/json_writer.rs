// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::Serializer;

use super::perfetto_types::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct PerfettoWriterJson<W>
where
    W: std::io::Write,
{
    out: W,
    entries: usize,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<W> PerfettoWriterJson<W>
where
    W: std::io::Write,
{
    pub fn new(mut out: W) -> Self {
        write!(out, "[").unwrap();
        Self { out, entries: 0 }
    }

    pub fn write_event(&mut self, event: PerfettoEvent<'_>) {
        if self.entries != 0 {
            writeln!(self.out, ",").unwrap();
        }

        let ph = match event.phase {
            Phase::Begin => "B",
            Phase::End => "E",
            Phase::Instant => "i",
            Phase::BeginAsync => "b",
            Phase::EndAsync => "e",
            Phase::InstantAsync => "n",
        };

        write!(self.out, "{{").unwrap();
        self.write_field_num("ts", event.ts, true);
        self.write_field_num("pid", event.pid, false);
        self.write_field_num("tid", event.tid, false);
        self.write_field_str_safe("ph", ph, false);
        if let Some(id) = event.id {
            self.write_field_num("id", id, false);
        }
        if let Some(name) = event.name {
            self.write_field_str("name", name, false);
        }
        if let Some(scope) = event.scope {
            let scope = match scope {
                InstantScope::Global => "g",
                InstantScope::Process => "p",
                InstantScope::Thread => "t",
            };
            self.write_field_str_safe("s", scope, false);
        }
        if let Some(args) = event.args {
            write!(self.out, r#","args":{{"#).unwrap();
            args.visit(&mut ArgsVisitor {
                out: &mut self.out,
                entries: 0,
            });
            write!(self.out, "}}").unwrap();
        }
        write!(self.out, "}}").unwrap();

        self.entries += 1;
    }

    pub fn write_metadata(&mut self, event: &PerfettoMetadata<'_>) {
        if self.entries != 0 {
            writeln!(self.out, ",").unwrap();
        }

        write!(self.out, "{{").unwrap();
        match event {
            PerfettoMetadata::ThreadName { tid, name } => {
                self.write_field_num("pid", 1, true);
                self.write_field_num("tid", tid, false);
                self.write_field_str_safe("ph", "M", false);
                self.write_field_str_safe("name", "thread_name", false);

                write!(self.out, r#","args":{{"#).unwrap();
                self.write_field_str("name", name, true);
                write!(self.out, "}}}}").unwrap();
            }
        }

        self.entries += 1;
    }

    pub fn finish(&mut self) {
        write!(self.out, "]").unwrap();
    }

    fn write_field_num<T: std::fmt::Display>(&mut self, key: &str, val: T, first: bool) {
        if !first {
            write!(self.out, ",").unwrap();
        }
        write!(self.out, r#""{key}":{val}"#).unwrap();
    }

    fn write_field_str(&mut self, key: &str, val: &str, first: bool) {
        if !first {
            write!(self.out, ",").unwrap();
        }
        write!(self.out, r#""{key}":"#).unwrap();
        write_str_escaped(&mut self.out, val);
    }

    fn write_field_str_safe(&mut self, key: &str, val: &str, first: bool) {
        if !first {
            write!(self.out, ",").unwrap();
        }
        write!(self.out, r#""{key}":"{val}""#).unwrap();
    }
}

struct ArgsVisitor<W> {
    out: W,
    entries: usize,
}

impl<W> tracing::field::Visit for ArgsVisitor<W>
where
    W: std::io::Write,
{
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if self.entries != 0 {
            write!(self.out, ",").unwrap();
        }

        write_str_escaped(&mut self.out, field.name());
        write!(self.out, ":").unwrap();

        // TODO: avoid allocating
        let value = format!("{value:?}");
        write_str_escaped(&mut self.out, &value);

        self.entries += 1;
    }
}

fn write_str_escaped<W: std::io::Write>(out: W, s: &str) {
    let mut ser = serde_json::Serializer::new(out);
    ser.serialize_str(s).unwrap();
}

impl<W> Drop for PerfettoWriterJson<W>
where
    W: std::io::Write,
{
    fn drop(&mut self) {
        self.finish();
    }
}
