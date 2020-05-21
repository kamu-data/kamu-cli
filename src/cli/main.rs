#[macro_use]
extern crate clap;

mod commands;

use clap::{App, AppSettings, Arg, Shell, SubCommand};
use commands::*;

fn cli() -> App<'static, 'static> {
    App::new("My Super Program")
        .global_settings(&[AppSettings::ColoredHelp])
        .settings(&[AppSettings::SubcommandRequiredElseHelp])
        .version("1.0")
        .about("Does awesome things")
        .arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity"),
        )
        .subcommand(
            SubCommand::with_name("log")
                .about("controls testing features")
                .arg(
                    Arg::with_name("dataset")
                        .required(true)
                        .index(1)
                        .help("asdasd"),
                ),
        )
        .subcommand(
            SubCommand::with_name("pull")
                .about("controls testing features")
                .arg(
                    Arg::with_name("dataset")
                        .required(true)
                        .index(1)
                        .help("asdasd"),
                ),
        )
        .subcommand(
            SubCommand::with_name("sql")
                .about("controls testing features")
                .subcommand(
                    SubCommand::with_name("server")
                        .about("controls testing features")
                        .arg(
                            Arg::with_name("address")
                                .long("address")
                                .default_value("127.0.0.1")
                                .help("asdasd"),
                        )
                        .arg(
                            Arg::with_name("port")
                                .long("port")
                                .default_value("8080")
                                .help("asdasd"),
                        ),
                ),
        )
        .subcommand(
            SubCommand::with_name("completions")
                .about("Generate tab-completion scripts for your shell")
                .after_help("HOWTOOOOOOOOOOOOOOOO")
                .arg(
                    Arg::with_name("shell")
                        .required(true)
                        .possible_values(&Shell::variants()),
                ),
        )
}

fn main() {
    let matches = cli().get_matches();

    let mut command: Box<dyn Command> = match matches.subcommand() {
        ("log", Some(_)) => Box::new(LogCommand::new()),
        ("pull", Some(_)) => Box::new(PullCommand::new()),
        ("sql", Some(submatches)) => match submatches.subcommand() {
            ("server", Some(server_matches)) => Box::new(SqlServerCommand::new(
                server_matches.value_of("address").unwrap(),
                value_t_or_exit!(server_matches.value_of("port"), u16),
            )),
            _ => panic!("Unrecognized command"),
        },
        ("completions", Some(submatches)) => {
            let shell = value_t_or_exit!(submatches.value_of("shell"), Shell);
            cli().gen_completions_to("kamu-rs", shell, &mut std::io::stdout());
            Box::new(NoOpCommand)
        }
        _ => panic!("Unrecognized command"),
    };

    command.run();
}
