use clap::value_t_or_exit;

mod cli_parser;
mod commands;

use commands::*;
use kamu::domain::*;

fn main() {
    let metadata_repo = MetadataRepository::new();

    let matches = cli_parser::cli().get_matches();

    let mut command: Box<dyn Command> = match matches.subcommand() {
        ("list", Some(_)) => Box::new(ListCommand::new(&metadata_repo)),
        ("log", Some(_)) => Box::new(LogCommand::new()),
        ("pull", Some(_)) => Box::new(PullCommand::new()),
        ("sql", Some(submatches)) => match submatches.subcommand() {
            ("", None) => Box::new(SqlShellCommand::new()),
            ("server", Some(server_matches)) => Box::new(SqlServerCommand::new(
                server_matches.value_of("address").unwrap(),
                value_t_or_exit!(server_matches.value_of("port"), u16),
            )),
            _ => panic!("Unrecognized command"),
        },
        ("completions", Some(submatches)) => {
            let shell = value_t_or_exit!(submatches.value_of("shell"), clap::Shell);
            cli_parser::cli().gen_completions_to("kamu-rs", shell, &mut std::io::stdout());
            Box::new(NoOpCommand)
        }
        _ => panic!("Unrecognized command"),
    };

    command.run();
}
