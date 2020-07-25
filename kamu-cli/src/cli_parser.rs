use clap::{App, AppSettings, Arg, Shell, SubCommand};

pub fn cli(binary_name: &'static str, version: &'static str) -> App<'static, 'static> {
    App::new(binary_name)
        .global_settings(&[AppSettings::ColoredHelp])
        .settings(&[AppSettings::SubcommandRequiredElseHelp])
        .version(version) // TODO: get true version
        .arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity (repeat for more)"),
        )
        .subcommands(vec![
            SubCommand::with_name("add")
                .about("Add a new dataset or modify existing one")
                .arg(
                    Arg::with_name("snapshot")
                        .multiple(true)
                        .index(1)
                        .help("Dataset snapshot reference(s) (path, URL, or remote)"),
                ),
            SubCommand::with_name("complete")
                .about("Completes a command in the shell")
                .arg(Arg::with_name("input").required(true).index(1))
                .arg(Arg::with_name("current").required(true).index(2)),
            SubCommand::with_name("completions")
                .about("Generate tab-completion scripts for your shell")
                .after_help("HOWTOOOOOOOOOOOOOOOO")
                .arg(
                    Arg::with_name("shell")
                        .required(true)
                        .possible_values(&Shell::variants()),
                ),
            SubCommand::with_name("init")
                .about("Initialize the workspace in the current directory")
                .arg(
                    Arg::with_name("pull_images")
                        .long("pull-images")
                        .help("Only pull docker images and exit"),
                ),
            SubCommand::with_name("list").about("List all datasets in the workspace"),
            SubCommand::with_name("log")
                .about("Show dataset's metadata history")
                .arg(
                    Arg::with_name("dataset")
                        .required(true)
                        .index(1)
                        .help("ID of the dataset"),
                ),
            SubCommand::with_name("pull")
                .about("Pull new data into the datasets")
                .arg(
                    Arg::with_name("all")
                        .short("a")
                        .long("all")
                        .help("Pull all datasets in the workspace"),
                )
                .arg(
                    Arg::with_name("recursive")
                        .short("r")
                        .long("recursive")
                        .help("Also pull all transitive dependencies of specified datasets"),
                )
                .arg(
                    Arg::with_name("dataset")
                        .multiple(true)
                        .index(1)
                        .help("Dataset ID(s)"),
                ),
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
        ])
}
