extern crate parquet;

use super::Command;

pub struct SqlShellCommand {}

impl SqlShellCommand {
    pub fn new() -> SqlShellCommand {
        SqlShellCommand {}
    }
}

impl Command for SqlShellCommand {
    fn run(&mut self) {
        println!("Vroooom Vroom!");
    }
}
