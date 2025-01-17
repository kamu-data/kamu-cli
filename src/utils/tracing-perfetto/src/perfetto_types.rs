// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub struct PerfettoEvent<'a> {
    pub ts: f64,
    pub tid: u64,
    pub pid: u64,
    pub phase: Phase,
    pub name: Option<&'a str>,
    pub id: Option<u64>,
    pub args: Option<Visitable<'a>>,
    pub scope: Option<InstantScope>,
}

// See: https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Begin,
    End,
    Instant,
    BeginAsync,
    EndAsync,
    InstantAsync,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstantScope {
    Global,
    Process,
    Thread,
}

#[derive(Debug)]
pub enum PerfettoMetadata<'a> {
    ThreadName { tid: u64, name: &'a str },
}

pub enum Visitable<'a> {
    Span(&'a tracing::span::Attributes<'a>),
    Event(&'a tracing::Event<'a>),
}

impl Visitable<'_> {
    pub fn visit(&self, v: &mut dyn tracing::field::Visit) {
        match self {
            Visitable::Span(attrs) => attrs.record(v),
            Visitable::Event(event) => event.record(v),
        }
    }
}
