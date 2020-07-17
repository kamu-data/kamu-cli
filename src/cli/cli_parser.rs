use clap::{App, AppSettings, Arg, Shell, SubCommand};

pub fn cli() -> App<'static, 'static> {
    App::new("kamu")
        .global_settings(&[AppSettings::ColoredHelp])
        .settings(&[AppSettings::SubcommandRequiredElseHelp])
        .version("1.0") // TODO: get true version
        .arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity (repeat for more)"),
        )
        .subcommand(SubCommand::with_name("list").about("List all datasets in the workspace"))
        .subcommand(
            SubCommand::with_name("log")
                .about("Show dataset's metadata history")
                .arg(
                    Arg::with_name("dataset")
                        .required(true)
                        .index(1)
                        .help("ID of the dataset"),
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
