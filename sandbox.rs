#!/usr/bin/env rust-script

//! ```cargo
//! [dependencies]
//! time = "0.1.25"
//! ```
fn main() {
    println!("{}", time::now().rfc822z());
}
