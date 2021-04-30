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
        .after_help(indoc::indoc!(
            r"
            To get help around individual commands use:
              kamu <command> -h
              kamu <command> <sub-command> -h
            "
        ))
        .subcommands(vec![
            SubCommand::with_name("add")
                .about("Add a new dataset or modify an existing one")
                .args(&[
                    Arg::with_name("recursive")
                        .short("r")
                        .long("recursive")
                        .help("Recursively search for all manifest in the specified directory"),
                    Arg::with_name("replace")
                        .long("replace")
                        .help("Delete and re-add datasets that already exist"),
                    Arg::with_name("manifest")
                        .multiple(true)
                        .required(true)
                        .index(1)
                        .help("Dataset manifest reference(s) (path, or URL)"),
                ])
                .after_help(indoc::indoc!(
                    r"
                    This command creates a new dataset from the provided DatasetSnapshot manifest.

                    Note that after kamu creates a dataset the changes in the source file will not have any effect unless you run the add command again. When you are experimenting with adding new dataset you currently may need to delete and re-add it multiple times until you get your parameters and schema right.

                    In future versions the add command will allow you to modify the structure of already existing datasets (e.g. changing schema in a compatible way).

                    ### Examples ###

                    Add a root/derivative dataset from local manifest:

                        kamu add org.example.data.yaml

                    Add datasets from all manifests found in the current directory:

                        kamu add --recursive .

                    Add a dataset from manifest hosted externally (e.g. on GihHub):

                        kamu add https://raw.githubusercontent.com/kamu-data/kamu-repo-contrib/master/io.exchangeratesapi.daily.usd-cad.yaml

                    To add a dataset from remote see `kamu pull --remote` command.
                    "
                )),
            SubCommand::with_name("complete")
                .about("Completes a command in the shell")
                .setting(AppSettings::Hidden)
                .arg(Arg::with_name("input").required(true).index(1))
                .arg(Arg::with_name("current").required(true).index(2)),
            SubCommand::with_name("completions")
                .about("Generate tab-completion scripts for your shell")
                .after_help(indoc::indoc!(
                    r"
                    The command outputs on `stdout`, allowing you to re-direct the output to the file of your choosing. Where you place the file will depend on which shell and which operating system you are using. Your particular configuration may also determine
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
                .args(&[
                    Arg::with_name("all")
                        .short("a")
                        .long("all")
                        .help("Delete all datasets in the workspace"),
                    Arg::with_name("recursive")
                        .short("r")
                        .long("recursive")
                        .help("Also delete all transitive dependencies of specified datasets"),
                    Arg::with_name("dataset")
                        .multiple(true)
                        .index(1)
                        .help("Dataset ID(s)"),
                    Arg::with_name("yes")
                        .short("y")
                        .long("yes")
                        .help("Don't ask for confirmation"),
                ])
                .after_help(indoc::indoc!(
                    r"
                    This command deletes the dataset from your workspace, including both metadata and the raw data.

                    Take great care when deleting root datasets. If you have not pushed your local changes to a remote - the data will be lost forever.

                    Deleting a derivative dataset is usually not a big deal, since they can always be reconstructed.
                    "
                )),
            SubCommand::with_name("init")
                .about("Initialize an empty workspace in the current directory")
                .arg(
                    Arg::with_name("pull-images")
                        .long("pull-images")
                        .help("Only pull docker images and exit"),
                )
                .after_help(indoc::indoc!(
                    r"
                    A workspace is where kamu stores all the important information about datasets (metadata) and in some cases raw data.

                    It is recommended to create one kamu workspace per data science project, grouping all related datasets together.

                    Initializing a workspace creates two directories:
                        .kamu - contains dataset metadata as well as all supporting files (configs, known remotes etc.)
                        .kamu.local - a local data volume where all raw data is stored
                    "
                )),
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
                .after_help(indoc::indoc!(
                    r"
                    ### Examples ###

                    To see a human-friendly list of datasets in your workspace:

                        kamu list

                    To get a machine-readable list of datasets:

                        kamu list -o csv
                    "
                ))
            ),
            SubCommand::with_name("log")
                .about("Show dataset's metadata history")
                .arg(
                    Arg::with_name("dataset")
                        .required(true)
                        .index(1)
                        .help("ID of the dataset"),
                )
                .after_help(indoc::indoc!(
                    r"
                    A log of a dataset is the contents of its metadata - a historical record of everything that ever influenced how data in the dataset currently looks like.

                    This includes events such as:
                    - Data ingestion / transformation
                    - Change of query
                    - Change of schema
                    - Change of source URL or other ingestion steps in a root dataset
                    "
                )),
            SubCommand::with_name("new")
                .about("Creates a new dataset manifest from a template")
                .args(&[
                    Arg::with_name("root")
                        .long("root")
                        .help("Create a root dataset"),
                    Arg::with_name("derivative")
                        .long("derivative")
                        .help("Create a derivative dataset"),
                    Arg::with_name("id")
                        .required(true)
                        .index(1)
                        .help("ID of the new dataset"),
                ])
                .after_help(indoc::indoc!(
                    r"
                    This command will create a dataset manifest from a template allowing you to customize the most relevant parts without having to remember the exact structure of the yaml file.

                    ### Examples ###

                    Create `org.example.data.yaml` file from template in the current directory:

                        kamu new org.example.data --root
                    "
                )),
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
                    Arg::with_name("force-uncacheable")
                        .long("force-uncacheable")
                        .help("Pull latest data from the uncacheable data sources"),
                    Arg::with_name("dataset")
                        .multiple(true)
                        .index(1)
                        .help("Dataset ID(s)"),
                    Arg::with_name("set-watermark")
                        .long("set-watermark")
                        .takes_value(true)
                        .value_name("TIME")
                        .help("Injects a manual watermark into the dataset to signify that \
                            no data is expected to arrive with event time that precedes it"),
                    Arg::with_name("remote")
                        .long("remote")
                        .takes_value(true)
                        .value_name("REMOTE")
                        .help("Specifies which remote to pull data from"),
                ])
                .after_help(indoc::indoc!(
                    r"
                    Pull is a generic command that lets you refresh data in any dataset. It will act differently depending on the type of dataset it works with In case of a root dataset it implies ingesting data from an external source. In case of a derivative - processing previously unseen data of its dependencies. In case

                    ### Examples ###

                    Fetch latest data in a specific dataset:

                        kamu pull org.example.data

                    Fetch latest data for the entire dependency tree of a dataset:

                        kamu pull --recursive org.example.derivative

                    Refresh data of all datasets in the workspace:

                        kamu pull --all

                    Fetch dataset from a remote:

                        kamu pull org.example.data --remote org.example

                    Advance the watermark of a dataset:

                        kamu --set-watermark 2020-01-01 org.example.data
                    "
                )),
            SubCommand::with_name("push")
                .about("Push local data into the remote repository")
                .args(&[
                    // Arg::with_name("all")
                    //     .short("a")
                    //     .long("all")
                    //     .help("Push all datasets in the workspace"),
                    // Arg::with_name("recursive")
                    //     .short("r")
                    //     .long("recursive")
                    //     .help("Also push all transitive dependencies of specified datasets"),
                    Arg::with_name("dataset")
                        .multiple(true)
                        .index(1)
                        .help("Dataset ID(s)"),
                    Arg::with_name("remote")
                        .long("remote")
                        .takes_value(true)
                        .value_name("REMOTE")
                        .help("Specifies which remote to push data into"),
                ])
                .after_help(indoc::indoc!(
                    r"
                    Use this command to share your new dataset or new data with others. All changes performed by this command are atomic and non-destructive. This command will analyze the state of the dataset at the remote and will only upload data and metadata that wasn't previously seen.

                    Similarly to git, if someone else modified the dataset concurrently with you - your push will be rejected and you will have to resolve the conflict.

                    ### Examples ###

                    Upload all new data in a dataset to a remote:

                        kamu push org.example.data --remote org.example
                    "
                )),
            SubCommand::with_name("reset")
                .about("Revert the dataset back to the specified state")
                .args(&[
                    Arg::with_name("dataset")
                        .required(true)
                        .index(1)
                        .help("ID of the dataset"),
                    Arg::with_name("hash")
                        .required(true)
                        .index(2)
                        .help("Hash of the block to reset to"),
                    Arg::with_name("yes")
                        .short("y")
                        .long("yes")
                        .help("Don't ask for confirmation"),
                ])
                .after_help(indoc::indoc!(
                    r"
                    Resetting a dataset to the specified block erases all metadata blocks that followed it and deletes all data added since that point. This can sometimes be useful to resolve conflicts, but otherwise should be used with care.

                    Keep in mind that blocks that were already pushed to a remote could've been already observed by other people, so resetting the history will not let you take that data back.
                    "
                )),
            SubCommand::with_name("remote")
                .about("Manage set of tracked repositories")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .subcommands(vec![
                    SubCommand::with_name("add")
                        .about("Adds a remote repository")
                        .after_help(indoc::indoc!(r"
                            For Local Filesystem basic remote use following URL formats:
                                file:///home/me/example/remote
                                file:///c:/Users/me/example/remote

                            For S3-compatible basic remote use following URL formats:
                                s3://bucket.my-company.example
                                s3+http://my-minio-server:9000/bucket
                                s3+https://my-minio-server:9000/bucket
                        "))
                        .args(&[
                            Arg::with_name("name")
                                .required(true)
                                .index(1)
                                .help("Local alias of the remote repository"),
                            Arg::with_name("url")
                                .required(true)
                                .index(2)
                                .help("URL of the remote repository"),
                        ]),
                    SubCommand::with_name("delete")
                        .about("Deletes a reference to remote repository")
                        .args(&[
                            Arg::with_name("all")
                                .short("a")
                                .long("all")
                                .help("Delete all known remotes"),
                            Arg::with_name("remote")
                                .multiple(true)
                                .index(1)
                                .help("Remote name(s)"),
                            Arg::with_name("yes")
                                .short("y")
                                .long("yes")
                                .help("Don't ask for confirmation"),
                        ]),
                    tabular_output_params(SubCommand::with_name("list")
                        .about("Lists known remote repositories")
                    ),
                ])
                .after_help(indoc::indoc!(
                    r"
                    Remotes are nodes on the network that let users exchange datasets. In the most basic form, a remote can simply be a location where the dataset files are hosted over one of the supported file or object-based data transfer protocols. The owner of a dataset will have push privileges to this location, while other participants can pull data from it.

                    ### Examples ###

                    Add S3 bucket as a remote:

                        kamu remote add example-remote s3://bucket.my-company.example

                    Show available remotes:

                        kamu remote list
                    "
                )),
            tabular_output_params(SubCommand::with_name("sql")
                .about("Executes an SQL query or drops you into an SQL shell")
                .subcommand(
                    SubCommand::with_name("server")
                        .about("Run JDBC server only")
                        .args(&[
                            Arg::with_name("address")
                                .long("address")
                                .default_value("127.0.0.1")
                                .help("Expose JDBC server on specific network interface"),
                            Arg::with_name("port")
                                .long("port")
                                .default_value("10000")
                                .help("Expose JDBC server on specific port"),
                        ]),
                )
                .args(&[
                    Arg::with_name("url")
                        .long("url")
                        .takes_value(true)
                        .value_name("URL")
                        .help("URL of a running JDBC server (e.g jdbc:hive2://example.com:10000)"),
                    Arg::with_name("command")
                        .short("c")
                        .long("command")
                        .takes_value(true)
                        .value_name("CMD")
                        .help("SQL command to run"),
                    Arg::with_name("script")
                        .long("script")
                        .takes_value(true)
                        .value_name("FILE")
                        .help("SQL script file to execute"),
                ])
                .after_help(indoc::indoc!(
                    r"
                    SQL shell allows you to explore data of all dataset in your workspace using one of the supported data processing engines. This can be a great way to prepare and test a query that you cal later turn into derivative dataset.

                    ### Examples ###

                    Drop into SQL shell:

                        kamu sql

                    Execute SQL command and return its output in CSV format:

                        kamu sql -c 'SELECT * FROM `org.example.data` LIMIT 10' -o csv

                    Run SQL server to use with external data processing tools:

                        kamu sql server --address 0.0.0.0 --port 8080

                    Connect to a remote SQL server:

                        kamu sql --url jdbc:hive2://example.com:10000

                    Note: Currently when connecting to a remote SQL kamu server you will need to manually instruct it to load datasets from the data files. This can be done using the following command:

                        CREATE TEMP VIEW `my.dataset` AS (SELECT * FROM parquet.`kamu_data/my.dataset`);
                    "
                ))
            ),
        ])
}
