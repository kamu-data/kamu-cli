use super::{Command, Error};

pub struct SqlServerCommand {}

impl SqlServerCommand {
    pub fn new(_address: &str, _port: u16) -> Self {
        Self {}
    }
}

impl Command for SqlServerCommand {
    fn run(&mut self) -> Result<(), Error> {
        unimplemented!()
    }
}
