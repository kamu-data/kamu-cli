use super::*;

pub struct SqlServerCommand {
    address: String,
    port: u16,
}

impl SqlServerCommand {
    pub fn new(address: &str, port: u16) -> SqlServerCommand {
        SqlServerCommand {
            address: address.to_owned(),
            port: port,
        }
    }
}

impl Command for SqlServerCommand {
    fn run(&mut self) {
        println!("Vroooom Vroom! {}:{}", self.address, self.port)
    }
}
