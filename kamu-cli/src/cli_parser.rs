use clap::{App, AppSettings, Arg, Shell, SubCommand};

fn tabular_output_params<'a, 'b>(app: App<'a, 'b>) -> App<'a, 'b> {
    app.args(&[
        Arg::with_name("output-format")
            .long("output-format")
            .short("o")
            .takes_value(true)
            .value_name("FMT")
            .possible_values(&[
                "table", // "vertical",
                "csv",
                // "tsv",
                // "xmlattrs",
                // "xmlelements",
                "json",
            ])
            .help("Format to display the results in"),
        /*Arg::with_name("no-color")
            .long("no-color")
            .help("Control whether color is used for display"),
        Arg::with_name("incremental")
            .long("incremental")
            .help("Display result rows immediately as they are fetched"),
        Arg::with_name("no-header")
            .long("no-header")
            .help("Whether to show column names in query results"),
        Arg::with_name("header-interval")
            .long("header-interval")
            .takes_value(true)
            .value_name("INT")
            .help("The number of rows between which headers are displayed"),
        Arg::with_name("csv-delimiter")
            .long("csv-delimiter")
            .takes_value(true)
            .value_name("DELIM")
            .help("Delimiter in the csv output format"),
        Arg::with_name("csv-quote-character")
            .long("csv-quote-character")
            .takes_value(true)
            .value_name("CHAR")
            .help("Quote character in the csv output format"),
        Arg::with_name("null-value")
            .long("null-value")
            .takes_value(true)
            .value_name("VAL")
            .help("Use specified string in place of NULL values"),
        Arg::with_name("number-format")
            .long("number-format")
            .takes_value(true)
            .value_name("FMT")
            .help("Format numbers using DecimalFormat pattern"),
        Arg::with_name("date-format")
            .long("date-format")
            .takes_value(true)
            .value_name("FMT")
            .help("Format dates using SimpleDateFormat pattern"),
        Arg::with_name("time-format")
            .long("time-format")
            .takes_value(true)
            .value_name("FMT")
            .help("Format times using SimpleDateFormat pattern"),
        Arg::with_name("timestamp-format")
            .long("timestamp-format")
            .takes_value(true)
            .value_name("FMT")
            .help("Format timestamps using SimpleDateFormat pattern"),*/
    ])
}

pub fn cli(binary_name: &'static str, version: &'static str) -> App<'static, 'static> {
    App::new(binary_name)
        .global_settings(&[AppSettings::ColoredHelp])
        .settings(&[AppSettings::SubcommandRequiredElseHelp])
        .version(version)
        .arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity (repeat for more)"),
        )
        .subcommands(vec![
            SubCommand::with_name("add")
                .about("Add a new dataset or modify an existing one")
                .arg(
                    Arg::with_name("recursive")
                        .short("r")
                        .long("recursive")
                        .help("Recursively search for all snapshots in the specified directory"),
                )
                .arg(
                    Arg::with_name("snapshot")
                        .multiple(true)
                        .required(true)
                        .index(1)
                        .help("Dataset snapshot reference(s) (path, URL, or remote)"),
                ),
            SubCommand::with_name("complete")
                .about("Completes a command in the shell")
                .setting(AppSettings::Hidden)
                .arg(Arg::with_name("input").required(true).index(1))
                .arg(Arg::with_name("current").required(true).index(2)),
            SubCommand::with_name("completions")
                .about("Generate tab-completion scripts for your shell")
                .after_help(indoc::indoc!(
                    r"
                    The script outputs on `stdout`, allowing you to re-direct the output to the file
                    of your choosing. Where you place the file will depend on which shell and which
                    operating system you are using. Your particular configuration may also determine
                    where these scripts need to be placed.

                    Here are some common set ups:

                    #### BASH ####

                    Simplest way to enable completions in bash is to append the following line
                    to your `~/.bashrc`:

                        source <(kamu completions bash)

                    You will need to reload your shell session (or execute the same command in your
                    current one) for changes to take effect.

                    Please contribute a guide for your favorite shell!
                    "
                ))
                .arg(
                    Arg::with_name("shell")
                        .required(true)
                        .possible_values(&Shell::variants()),
                ),
            SubCommand::with_name("delete")
                .about("Delete a dataset")
                .arg(
                    Arg::with_name("all")
                        .short("a")
                        .long("all")
                        .help("Delete all datasets in the workspace"),
                )
                .arg(
                    Arg::with_name("recursive")
                        .short("r")
                        .long("recursive")
                        .help("Also delete all transitive dependencies of specified datasets"),
                )
                .arg(
                    Arg::with_name("dataset")
                        .multiple(true)
                        .index(1)
                        .help("Dataset ID(s)"),
                )
                .arg(
                    Arg::with_name("yes")
                        .short("y")
                        .long("yes")
                        .help("Don't ask for confirmation"),
                ),
            SubCommand::with_name("init")
                .about("Initialize an empty workspace in the current directory")
                .arg(
                    Arg::with_name("pull-images")
                        .long("pull-images")
                        .help("Only pull docker images and exit"),
                ),
            tabular_output_params(SubCommand::with_name("list")
                .about("List all datasets in the workspace")
                .subcommand(
                    SubCommand::with_name("depgraph")
                    .about("Outputs the dependency graph of datasets")
                    .after_help(indoc::indoc!(
                        r"
                        The output is in graphviz (dot) format.

                        If you have graphviz installed you can visualize the graph by running:

                            kamu list depgraph | dot -Tpng > depgraph.png
                        "
                    )),
                )
            ),
            SubCommand::with_name("log")
                .about("Show dataset's metadata history")
                .arg(
                    Arg::with_name("dataset")
                        .required(true)
                        .index(1)
                        .help("ID of the dataset"),
                ),
            SubCommand::with_name("new")
                .about("Creates a new dataset manifest")
                .arg(
                    Arg::with_name("root")
                        .long("root")
                        .help("Create a root dataset")
                )
                .arg(
                    Arg::with_name("derivative")
                        .long("derivative")
                        .help("Create a derivative dataset"),
                )
                .arg(
                    Arg::with_name("id")
                        .required(true)
                        .index(1)
                        .help("ID of the new dataset"),
                ),
            SubCommand::with_name("notebook")
                .about("Starts the notebook server for exploring the data in the workspace")
                .after_help(indoc::indoc!(
                    r"
                    This command will run the Jupyter server and the Spark engine connected together,
                    letting you query data with SQL before pulling it into the notebook for final
                    processing and visualization.

                    For more information check out notebook examples at https://github.com/kamu-data/kamu-cli
                    "
                ))
                .arg(
                    Arg::with_name("env")
                        .short("e")
                        .long("env")
                        .takes_value(true)
                        .value_name("VAR")
                        .multiple(true)
                        .help("Pass specified environment variable into the notebook (e.g. `-e VAR` or `-e VAR=foo`)"),
                ),
            SubCommand::with_name("pull")
                .about("Pull new data into the datasets")
                .args(&[
                    Arg::with_name("all")
                        .short("a")
                        .long("all")
                        .help("Pull all datasets in the workspace"),
                    Arg::with_name("recursive")
                        .short("r")
                        .long("recursive")
                        .help("Also pull all transitive dependencies of specified datasets"),
                    Arg::with_name("dataset")
                        .multiple(true)
                        .index(1)
                        .help("Dataset ID(s)"),
                    Arg::with_name("set-watermark")
                        .long("set-watermark")
                        .takes_value(true)
                        .value_name("T")
                        .help("Injects a manual watermark into the dataset to signify that \
                            no data is expected to arrive with event time that precedes it")
                ]),
            SubCommand::with_name("reset")
                .about("Revert the dataset back to the specified state")
                .arg(
                    Arg::with_name("dataset")
                        .required(true)
                        .index(1)
                        .help("ID of the dataset")
                )
                .arg(
                    Arg::with_name("hash")
                        .required(true)
                        .index(2)
                        .help("Hash of the block to reset to")
                )
                .arg(
                    Arg::with_name("yes")
                        .short("y")
                        .long("yes")
                        .help("Don't ask for confirmation")
                ),
            tabular_output_params(SubCommand::with_name("sql")
                .about("Executes an SQL query or drops you into an SQL shell")
                .subcommand(
                    SubCommand::with_name("server")
                        .about("Run JDBC server only")
                        .arg(
                            Arg::with_name("address")
                                .long("address")
                                .default_value("127.0.0.1")
                                .help("Expose JDBC server on specific network interface"),
                        )
                        .arg(
                            Arg::with_name("port")
                                .long("port")
                                .default_value("8080")
                                .help("Expose JDBC server on specific port"),
                        ),
                )
                .arg(
                    Arg::with_name("url")
                        .long("url")
                        .takes_value(true)
                        .value_name("URL")
                        .help("URL of a running JDBC server (e.g jdbc:hive2://example.com:10090)"),
                )
                .arg(
                    Arg::with_name("command")
                        .short("c")
                        .long("command")
                        .takes_value(true)
                        .value_name("CMD")
                        .help("SQL command to run"),
                )
                .arg(
                    Arg::with_name("script")
                        .long("script")
                        .takes_value(true)
                        .value_name("FILE")
                        .help("SQL script file to execute"),
                )
            ),
        ])
}
