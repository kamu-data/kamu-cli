// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use tracing::Instrument;
use tracing_perfetto::*;

#[tokio::main]
async fn main() {
    let _g = configure_tracing();
    root().await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tracing::instrument]
async fn root() {
    tracing::info!("Started");

    serial().await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    concurrent().await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    parallel().await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    parallel_linked().await;

    tracing::info!("Finished");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tracing::instrument]
async fn serial() {
    std::thread::sleep(Duration::from_millis(10));
    tokio::time::sleep(Duration::from_millis(50)).await;
    serial_1().await;
    serial_2().await;
}

#[tracing::instrument(fields(foo = "bar"))]
async fn serial_1() {
    std::thread::sleep(Duration::from_millis(10));
    tracing::info!(some = "field", "Test from serial_1");
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tracing::instrument(fields(bar = "baz"))]
async fn serial_2() {
    std::thread::sleep(Duration::from_millis(10));
    tracing::info!(some = "otherfield", "Test from serial_2");
    tokio::time::sleep(Duration::from_millis(100)).await;
    serial_2_1().await;
    serial_2_2().await;
}

#[tracing::instrument]
async fn serial_2_1() {
    std::thread::sleep(Duration::from_millis(10));
    tracing::info!("Test from serial_2_1");
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tracing::instrument]
async fn serial_2_2() {
    std::thread::sleep(Duration::from_millis(10));
    tracing::info!("Test from serial_2_2");
    tokio::time::sleep(Duration::from_millis(200)).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tracing::instrument]
async fn concurrent() {
    std::thread::sleep(Duration::from_millis(10));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let f1 = concurrent_1();
    let f2 = concurrent_2();
    tracing::info!("Test");
    tokio::join!(f1, f2);
}

#[tracing::instrument]
async fn concurrent_1() {
    std::thread::sleep(Duration::from_millis(10));
    tokio::time::sleep(Duration::from_millis(50)).await;
    concurrent_1_1().await;
    concurrent_1_2().await;
}

#[tracing::instrument]
async fn concurrent_1_1() {
    std::thread::sleep(Duration::from_millis(10));
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tracing::instrument]
async fn concurrent_1_2() {
    std::thread::sleep(Duration::from_millis(10));
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tracing::instrument]
async fn concurrent_2() {
    std::thread::sleep(Duration::from_millis(10));
    tokio::time::sleep(Duration::from_millis(100)).await;
    std::thread::sleep(Duration::from_millis(10));
    tokio::time::sleep(Duration::from_millis(100)).await;
    std::thread::sleep(Duration::from_millis(10));
    tokio::time::sleep(Duration::from_millis(200)).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tracing::instrument]
async fn parallel() {
    std::thread::sleep(Duration::from_millis(10));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let f1 = tokio::task::spawn(parallel_1());
    let f2 = tokio::task::spawn(parallel_2());
    tracing::info!("Test");
    let (r1, r2) = tokio::join!(f1, f2);
    r1.unwrap();
    r2.unwrap();
}

#[tracing::instrument]
async fn parallel_1() {
    std::thread::sleep(Duration::from_millis(10));
    tokio::time::sleep(Duration::from_millis(100)).await;
    parallel_1_1().await;
    parallel_1_2().await;
}

#[tracing::instrument]
async fn parallel_1_1() {
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tracing::instrument]
async fn parallel_1_2() {
    tokio::time::sleep(Duration::from_millis(200)).await;
}

#[tracing::instrument]
async fn parallel_2() {
    std::thread::sleep(Duration::from_millis(10));
    tokio::time::sleep(Duration::from_millis(200)).await;
    parallel_2_1().await;
    parallel_2_2().await;
}

#[tracing::instrument]
async fn parallel_2_1() {
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tracing::instrument]
async fn parallel_2_2() {
    tokio::time::sleep(Duration::from_millis(200)).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tracing::instrument]
async fn parallel_linked() {
    std::thread::sleep(Duration::from_millis(10));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let f1 = tokio::task::spawn(parallel_1().instrument(tracing::info_span!("task1").or_current()));
    let f2 = tokio::task::spawn(parallel_2().instrument(tracing::info_span!("task2").or_current()));
    let (r1, r2) = tokio::join!(f1, f2);
    r1.unwrap();
    r2.unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn configure_tracing() -> FlushGuard {
    //tracing_chrome::FlushGuard {
    use tracing_subscriber::fmt::format::FmtSpan;
    use tracing_subscriber::layer::SubscriberExt;

    // Use configuration from RUST_LOG env var if provided
    //let env_filter = EnvFilter::from_default_env();

    // Configure Perfetto tracing
    // let (perfetto_layer, perfetto_guard) =
    // tracing_chrome::ChromeLayerBuilder::new().file("trace.json")
    //     .trace_style(tracing_chrome::TraceStyle::Async)
    //     .include_locations(true)
    //     .include_args(true)
    //     .build();

    let (perfetto_layer, perfetto_guard) = PerfettoLayer::new("trace.json");

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_writer(std::io::stderr)
        .pretty();

    let subscriber = tracing_subscriber::registry()
        .with(perfetto_layer)
        .with(fmt_layer);

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");

    perfetto_guard
}
