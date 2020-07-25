use super::{Command, Error};

pub struct SqlShellCommand {}

impl SqlShellCommand {
    pub fn new() -> Self {
        Self {}
    }
}

impl Command for SqlShellCommand {
    fn run(&mut self) -> Result<(), Error> {
        println!("Vroooom Vroom!");
        Ok(())
    }
}
