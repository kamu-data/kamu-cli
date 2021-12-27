// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use clap::{App, AppSettings, Arg, Shell, SubCommand};
use opendatafabric::*;

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
                "json", "json-ld", "json-soa",
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

pub fn cli() -> App<'static, 'static> {
    App::new(crate::BINARY_NAME)
        .global_settings(&[AppSettings::ColoredHelp])
        .settings(&[AppSettings::SubcommandRequiredElseHelp])
        .version(crate::VERSION)
        .args(&[
            Arg::with_name("verbose")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity (repeat for more)"),
            Arg::with_name("quiet")
                .long("quiet")
                .short("q")
                .help("Suppress all non-essential output"),
        ])
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

                        kamu add https://raw.githubusercontent.com/kamu-data/kamu-repo-contrib/master/ca.bankofcanada.exchange-rates.daily.yaml

                    To add dataset from a repository see `kamu pull` command.
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

                    Append the following to your `~/.bashrc`:

                        source <(kamu completions bash)

                    You will need to reload your shell session (or execute the same command in your
                    current one) for changes to take effect.

                    #### ZSH ####

                    Append the following to your `~/.zshrc`:

                        autoload -U +X bashcompinit && bashcompinit
                        source <(kamu completions bash)

                    Please contribute a guide for your favorite shell!
                    "
                ))
                .arg(
                    Arg::with_name("shell")
                        .required(true)
                        .possible_values(&Shell::variants()),
                ),
            SubCommand::with_name("config")
                .about("Get or set configuration options")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .subcommands(vec![
                    SubCommand::with_name("list")
                        .about("Display current configuration combined from all config files")
                        .args(&[
                            Arg::with_name("user")
                                .long("user")
                                .help("Show only user scope configuration"),
                            Arg::with_name("with-defaults")
                                .long("with-defaults")
                                .help("Show configuration with all default values applied"),
                        ]),
                    SubCommand::with_name("get")
                        .about("Get current configuration value")
                        .args(&[
                            Arg::with_name("user")
                                .long("user")
                                .help("Operate on the user scope configuration file"),
                            Arg::with_name("with-defaults")
                                .long("with-defaults")
                                .help("Get default value if config option is not explicitly set"),
                            Arg::with_name("cfgkey")
                                .required(true)
                                .index(1)
                                .help("Path to the config option"),
                        ]),
                    SubCommand::with_name("set")
                        .about("Set or unset configuration value")
                        .args(&[
                            Arg::with_name("user")
                                .long("user")
                                .help("Operate on the user scope configuration file"),
                            Arg::with_name("cfgkey")
                                .required(true)
                                .index(1)
                                .help("Path to the config option"),
                            Arg::with_name("value")
                                .index(2)
                                .help("New value to set"),
                        ]),
                ])
                .after_help(indoc::indoc!(
                    r"
                    Configuration in `kamu` is managed very similarly to `git`. Starting with your current workspace and going up the directory tree you can have multiple `.kamuconfig` YAML files which are all merged together to get the resulting config. 
                    
                    Most commonly you will have a workspace-scoped config inside the `.kamu` directory and the user-scoped config residing in your home directory.

                    ### Examples ###

                    List current configuration as combined view of config files:

                        kamu config list

                    Get current configuration value:

                        kamu config get engine.runtime

                    Set configuration value in workspace scope:

                        kamu config set engine.runtime podman
                    
                    Set configuration value in user scope:

                        kamu config set --user engine.runtime podman

                    Unset or revert to default value:

                        kamu config set --user engine.runtime
                    "
                )),
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
                        .validator(validate_dataset_ref_any)
                        .help("Local or remote dataset reference(s)"),
                    Arg::with_name("yes")
                        .short("y")
                        .long("yes")
                        .help("Don't ask for confirmation"),
                ])
                .after_help(indoc::indoc!(
                    r"
                    This command deletes the dataset from your workspace, including both metadata and the raw data.

                    Take great care when deleting root datasets. If you have not pushed your local changes to a repository - the data will be lost.

                    Deleting a derivative dataset is usually not a big deal, since they can always be reconstructed, but will disrupt downstream consumers.

                    ### Examples ###

                    Delete a local dataset:

                        kamu delete my.dataset

                    Delete a dataset in remote repository (some repos may not allow this):

                        kamu delete kamu.dev/my.dataset
                    "
                )),
            SubCommand::with_name("init")
                .about("Initialize an empty workspace in the current directory")
                .args(&[
                    Arg::with_name("pull-images")
                        .long("pull-images")
                        .help("Only pull container images and exit"),
                    Arg::with_name("pull-test-images")
                        .long("pull-test-images")
                        .hidden(true)
                        .help("Only pull test-related container images and exit"),
                    Arg::with_name("list-only")
                        .long("list-only")
                        .hidden(true)
                        .help("List image names instead of pulling")
                ])
                .after_help(indoc::indoc!(
                    r"
                    A workspace is where kamu stores all the important information about datasets (metadata) and in some cases raw data.

                    It is recommended to create one kamu workspace per data science project, grouping all related datasets together.

                    Initializing a workspace creates two directories:
                        .kamu - contains dataset metadata as well as all supporting files (configs, known repositories etc.)
                        .kamu.local - a local data volume where all raw data is stored
                    "
                )),
            SubCommand::with_name("inspect")
                .about("Group of commands for exploring dataset metadata")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .subcommands(vec![
                    SubCommand::with_name("lineage")
                        .about("Shows the dependency tree of a dataset")
                        .args(&[
                            Arg::with_name("output-format")
                                .long("output-format")
                                .short("o")
                                .takes_value(true)
                                .value_name("FMT")
                                .possible_values(&[
                                    "shell", "dot", "csv", "html"
                                ])
                                .help("Format of an output"),
                            Arg::with_name("browse")
                                .long("browse")
                                .short("b")
                                .help("Produce HTML and open it in a browser"),
                            Arg::with_name("dataset")
                                .multiple(true)
                                .index(1)
                                .validator(validate_dataset_ref_local)
                                .help("Local dataset reference(s)"),
                        ])
                        .after_help(indoc::indoc!(
                            r"
                            Presents the dataset-level lineage that includes current and past dependencies.

                            ### Examples ###

                            Show lineage of a single dataset:

                                kamu inspect lineage my.dataset

                            Show lineage graph of all datasets in a browser:

                                kamu inspect lineage -b
                            
                            Render the lineage graph into a png image (needs graphviz installed):

                                kamu inspect lineage -o dot | dot -Tpng > depgraph.png
                            "
                        )),
                    SubCommand::with_name("query")
                        .about("Shows the transformations used by a derivative dataset")
                        .args(&[
                            Arg::with_name("dataset")
                                .required(true)
                                .index(1)
                                .validator(validate_dataset_ref_local)
                                .help("Local dataset reference"),
                        ])
                        .after_help(indoc::indoc!(
                            r"
                            This command allows you to audit the transformations performed by a derivative dataset and their evolution. Such audit is an important step in validating the trustworthiness of data (see `kamu verify` command).
                            "
                        )),
                    SubCommand::with_name("schema")
                        .about("Shows the dataset schema")
                        .args(&[
                            Arg::with_name("dataset")
                                .required(true)
                                .index(1)
                                .validator(validate_dataset_ref_local)
                                .help("Local dataset reference"),
                            Arg::with_name("output-format")
                                .long("output-format")
                                .short("o")
                                .takes_value(true)
                                .value_name("FMT")
                                .possible_values(&[
                                    "ddl", "parquet"
                                ])
                                .help("Format of an output"),
                        ])
                        .after_help(indoc::indoc!(
                            r"
                            Displays the schema of the dataset. Note that dataset schemas can evolve over time and by default the latest schema will be shown.

                            ### Examples ###

                            Show logical schema of a dataset in the DDL format:

                                kamu inspect schema my.dataset

                            Show physical schema of the underlying Parquet files:

                                kamu inspect schema my.dataset -o parquet
                            "
                        )),
                ]),
            tabular_output_params(SubCommand::with_name("list")
                .about("List all datasets in the workspace")
                .args(&[
                    Arg::with_name("wide")
                    .short("w")
                    .multiple(true)
                    .help("Show more details (repeat for more)"),
                ])
                .after_help(indoc::indoc!(
                    r"
                    ### Examples ###

                    To see a human-friendly list of datasets in your workspace:

                        kamu list

                    To see more details:

                        kamu list -w

                    To get a machine-readable list of datasets:

                        kamu list -o csv
                    "
                ))
            ),
            SubCommand::with_name("log")
                .about("Shows dataset metadata history")
                .args(&[
                    Arg::with_name("dataset")
                        .required(true)
                        .index(1)
                        .validator(validate_dataset_ref_local)
                        .help("Local dataset reference"),
                    Arg::with_name("output-format")
                        .long("output-format")
                        .short("o")
                        .takes_value(true)
                        .value_name("FMT")
                        .possible_values(&[
                            "yaml",
                        ]),
                    Arg::with_name("filter")
                        .long("filter")
                        .short("f")
                        .takes_value(true)
                        .value_name("FLT")
                        .validator(validate_log_filter),
                ])
                .after_help(indoc::indoc!(
                    r"
                    Metadata of a dataset contains historical record of everything that ever influenced how data currently looks like.

                    This includes events such as:
                    - Data ingestion / transformation
                    - Change of query
                    - Change of schema
                    - Change of source URL or other ingestion steps in a root dataset

                    Use this command to explore how dataset evolved over time.

                    ### Examples ###

                    Show brief summaries of individual metadata blocks:

                        kamu log org.example.data

                    Show detailed content of all blocks:

                        kamu log -o yaml org.example.data

                    Using a filter to inspect blocks containing query changes of a derivative dataset:

                        kamu log -o yaml --filter source org.example.data
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
                    Arg::with_name("name")
                        .required(true)
                        .index(1)
                        .validator(validate_dataset_name)
                        .help("Name of the new dataset"),
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
                        .validator(validate_dataset_ref_any)
                        .help("Local or remote dataset reference(s)"),
                    Arg::with_name("as")
                        .long("as")
                        .takes_value(true)
                        .validator(validate_dataset_name)
                        .value_name("NAME")
                        .help("Local name of a dataset to use when syncing from a repository"),
                    Arg::with_name("fetch")
                        .long("fetch")
                        .takes_value(true)
                        .value_name("SRC")
                        .help("Data location (path or URL)"),
                    Arg::with_name("set-watermark")
                        .long("set-watermark")
                        .takes_value(true)
                        .value_name("TIME")
                        .help("Injects a manual watermark into the dataset to signify that \
                            no data is expected to arrive with event time that precedes it"),
                ])
                .after_help(indoc::indoc!(
                    r"
                    Pull is a multi-functional command that lets you make changes to a local dataset. Depending on the parameters and the types of datasets involved it can be used to:
                    - Ingest new data into a root dataset from an external source
                    - Run transformations on a derivative dataset to process previously unseen data
                    - Pull dataset from a remote repository into your workspace
                    - Update watermark on a dataset

                    ### Examples ###

                    Fetch latest data in a specific dataset:

                        kamu pull org.example.data

                    Fetch latest data for the entire dependency tree of a dataset:

                        kamu pull --recursive org.example.derivative

                    Refresh data of all datasets in the workspace:

                        kamu pull --all

                    Fetch dataset from a repository:

                        kamu pull kamu.dev/foobar/org.example.data
                    
                    Fetch dataset from a repository using a different local name:

                        kamu pull kamu.dev/foobar/org.example.data --as my.data

                    Advance the watermark of a dataset:

                        kamu pull --set-watermark 2020-01-01 org.example.data

                    Ingest data into the root dataset from file or URL (format should match one expected by the 'prepare' step):

                        kamu pull org.example.data --fetch path/to/data.csv
                        kamu pull org.example.data --fetch https://example.com/data.csv
                    "
                )),
            SubCommand::with_name("push")
                .about("Push local data into a repository")
                .args(&[
                    Arg::with_name("all")
                        .short("a")
                        .long("all")
                        .help("Push all datasets in the workspace"),
                    Arg::with_name("recursive")
                        .short("r")
                        .long("recursive")
                        .help("Also push all transitive dependencies of specified datasets"),
                    Arg::with_name("dataset")
                        .multiple(true)
                        .index(1)
                        .validator(validate_dataset_ref_any)
                        .help("Local or remote dataset reference(s)"),
                    Arg::with_name("as")
                        .long("as")
                        .takes_value(true)
                        .validator(validate_dataset_remote_name)
                        .value_name("REM")
                        .help("Remote alias to use when pushing"),
                ])
                .after_help(indoc::indoc!(
                    r"
                    Use this command to share your new dataset or new data with others. All changes performed by this command are atomic and non-destructive. This command will analyze the state of the dataset at the repository and will only upload data and metadata that wasn't previously seen.

                    Similarly to git, if someone else modified the dataset concurrently with you - your push will be rejected and you will have to resolve the conflict.

                    ### Examples ###

                    Push dataset to a repository and create an association between the two:

                        kamu push org.example.data --as kamu.dev/me/org.example.data

                    Push dataset previously associated with a repository:

                        kamu push org.example.data
                    "
                )),
            /*SubCommand::with_name("reset")
                .about("Revert the dataset back to the specified state")
                .args(&[
                    Arg::with_name("dataset")
                        .required(true)
                        .index(1)
                        .validator(validate_dataset_id)
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

                    Keep in mind that blocks that were already pushed to a repository could've been already observed by other people, so resetting the history will not let you take that data back.
                    "
                )),*/
            SubCommand::with_name("repo")
                .about("Manage set of tracked repositories")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .subcommands(vec![
                    tabular_output_params(SubCommand::with_name("list")
                        .about("Lists known repositories")
                    ),
                    SubCommand::with_name("add")
                        .about("Adds a repository")
                        .after_help(indoc::indoc!(r"
                            For Local Filesystem basic repository use following URL formats:
                                file:///home/me/example/repository
                                file:///c:/Users/me/example/repository

                            For S3-compatible basic repository use following URL formats:
                                s3://bucket.my-company.example
                                s3+http://my-minio-server:9000/bucket
                                s3+https://my-minio-server:9000/bucket
                        "))
                        .args(&[
                            Arg::with_name("name")
                                .required(true)
                                .index(1)
                                .validator(validate_repository_name)
                                .help("Local alias of the repository"),
                            Arg::with_name("url")
                                .required(true)
                                .index(2)
                                .help("URL of the repository"),
                        ]),
                    SubCommand::with_name("delete")
                        .about("Deletes a reference to repository")
                        .args(&[
                            Arg::with_name("all")
                                .short("a")
                                .long("all")
                                .help("Delete all known repositories"),
                            Arg::with_name("repository")
                                .multiple(true)
                                .index(1)
                                .validator(validate_repository_name)
                                .help("Repository name(s)"),
                            Arg::with_name("yes")
                                .short("y")
                                .long("yes")
                                .help("Don't ask for confirmation"),
                        ]),
                    tabular_output_params(SubCommand::with_name("list")
                        .about("Lists known repositories")
                    ),
                    SubCommand::with_name("alias")
                        .about("Manage set of remote aliases associated with datasets")
                        .setting(AppSettings::SubcommandRequiredElseHelp)
                        .subcommands(vec![
                            tabular_output_params(SubCommand::with_name("list")
                                .about("Lists remote aliases")
                                .args(&[
                                    Arg::with_name("dataset")
                                        .index(1)
                                        .validator(validate_dataset_ref_local)
                                        .help("Local dataset reference"),
                                ])
                            ),
                            SubCommand::with_name("add")
                                .about("Adds a remote alias to a dataset")
                                .args(&[
                                    Arg::with_name("dataset")
                                        .required(true)
                                        .index(1)
                                        .validator(validate_dataset_ref_local)
                                        .help("Local dataset reference"),
                                    Arg::with_name("alias")
                                        .required(true)
                                        .index(2)
                                        .validator(validate_dataset_remote_name)
                                        .help("Remote dataset name"),
                                    Arg::with_name("push")
                                        .long("push")
                                        .help("Add a push alias"),
                                    Arg::with_name("pull")
                                        .long("pull")
                                        .help("Add a pull alias"),
                                ]),
                            SubCommand::with_name("delete")
                                .about("Deletes a remote alias associated with a dataset")
                                .args(&[
                                    Arg::with_name("all")
                                        .short("a")
                                        .long("all")
                                        .help("Delete all aliases"),
                                    Arg::with_name("dataset")
                                        .required(true)
                                        .index(1)
                                        .validator(validate_dataset_ref_local)
                                        .help("Local dataset reference"),
                                    Arg::with_name("alias")
                                        .index(2)
                                        .validator(validate_dataset_remote_name)
                                        .help("Remote dataset name"),
                                    Arg::with_name("push")
                                        .long("push")
                                        .help("Add a push alias"),
                                    Arg::with_name("pull")
                                        .long("pull")
                                        .help("Add a pull alias"),
                                ]),
                        ])
                        .after_help(indoc::indoc!(
                            r"
                            When you pull and push datasets from repositories kamu uses aliases to let you avoid specifying the full remote referente each time. Aliases are usually created the first time you do a push or pull and saved for later. If you have an unusual setup (e.g. pushing to multiple repositories) you can use this command to manage the aliases.

                            ### Examples ###

                            List all aliases:

                                kamu repo alias list

                            List all aliases of a specific dataset:

                                kamu repo alias list org.example.data

                            Add a new pull alias:

                                kamu repo alias add --pull org.example.data kamu.dev/me/org.example.data
                            "
                        )),
                ])
                .after_help(indoc::indoc!(
                    r"
                    Repositories are nodes on the network that let users exchange datasets. In the most basic form, a repository can simply be a location where the dataset files are hosted over one of the supported file or object-based data transfer protocols. The owner of a dataset will have push privileges to this location, while other participants can pull data from it.

                    ### Examples ###

                    Show available repositories:

                        kamu repo list

                    Add S3 bucket as a repository:

                        kamu repo add example-repo s3://bucket.my-company.example
                    "
                )),
                tabular_output_params(SubCommand::with_name("search")
                .about("Searches for datasets in the registered repositories")
                .args(&[
                    Arg::with_name("query")
                        .index(1)
                        .value_name("QUERY")
                        .help("Search terms"),
                    Arg::with_name("repo")
                        .long("repo")
                        .multiple(true)
                        .value_name("REPO")
                        .validator(validate_repository_name)
                        .help("Repository name(s) to search in"),
                ])
                .after_help(indoc::indoc!(
                    r"
                    Search is delegated to the repository implementations and its capabilities depend on the type of the repo. Whereas smart repos may support advanced full-text search, simple storage-only repos may be limited to a substring search by DatasetID.

                    ### Examples ###

                    Search all repositories:

                        kamu search covid19

                    Search only specific repositories:

                        kamu search covid19 --repo kamu --repo statcan.gc.ca
                    "
                ))
            ),
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
                            Arg::with_name("livy")
                                .long("livy")
                                .help("Run Livy server instead of JDBC")
                                .hidden(true),
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
                    Arg::with_name("engine")
                        .long("engine")
                        .takes_value(true)
                        .possible_values(&["spark", "datafusion"])
                        .value_name("ENG")
                        .help("Engine type to use for this SQL session"),
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
            tabular_output_params(SubCommand::with_name("tail")
                .about("Displays a sample of most recent records in a dataset")
                .args(&[
                    Arg::with_name("dataset")
                        .required(true)
                        .index(1)
                        .validator(validate_dataset_ref_local)
                        .help("Local dataset reference"),
                    Arg::with_name("num-records")
                        .long("num-records")
                        .short("n")
                        .takes_value(true)
                        .default_value("10")
                        .value_name("NUM")
                        .help("Number of records to display"),
                ])
                .after_help(indoc::indoc!(
                    r#"
                    This command is simply a shortcut for:

                        kamu sql --engine datafusion --command 'SELECT * FROM "{dataset}" ORDER BY {offset_col} DESC LIMIT {num_records}'
                    "#
                ))
            ),
            SubCommand::with_name("verify")
                .about("Verifies the validity of a dataset")
                .args(&[
                    Arg::with_name("recursive")
                        .short("r")
                        .long("recursive")
                        .help("Verify the entire transformation chain starting with root datasets"),
                    Arg::with_name("integrity")
                        .long("integrity")
                        .help("Check only the hashes of metadata and data without replaying transformations"),
                    Arg::with_name("dataset")
                        .multiple(true)
                        .index(1)
                        .validator(validate_dataset_ref_local)
                        .help("Local dataset reference(s)"),
                ])
                .after_help(indoc::indoc!(
                    r"
                    Validity of derivative data is determined by:
                    - Trustworthiness of the source data that went into it
                    - Soundness of the derivative transformation chain that shaped it
                    - Guaranteeing that derivative data was in fact produced by declared transformations

                    For the first two, you can inspect the dataset lineage so see which root datasets the data is coming from and whether their publishers are credible. Then you can audit all derivative transformations to ensure they are sound and non-malicious.

                    This command can help you with the last stage. It uses the history of transformations stored in metadata to first compare the hashes of data with ones stored in metadata (i.e. verify that data corresponds to metadata). Then it repeats all declared transformations locally to ensure that what's declared in metadata actually produces the presented result.

                    The combination of the above steps can give you a high certainty that the data you're using is trustworthy.

                    When called on a root dataset the command will only perform the integrity check of comparing data hashes to metadata.

                    ### Examples ###

                    Verify the data in a dataset starting from its immediate inputs:

                        kamu verify com.example.deriv

                    Verify the entire transformation chain starting with root datasets (may download a lot of data):

                        kamu pull --recursive com.example.deriv

                    Verify only the hashes of metadata and data, without replaying the transformations. This is useful when you trust the peers performing transformations but want to ensure data was not tampered in storage or during the transmission:

                        kamu verify --integrity com.example.deriv
                    "
                )),
        ])
}

fn validate_dataset_name(s: String) -> Result<(), String> {
    match DatasetName::try_from(&s) {
        Ok(_) => Ok(()),
        Err(_) => Err(format!(
            "Dataset name can only contain alphanumerics, dashes, and dots, e.g. `my.dataset-id`",
        )),
    }
}

fn validate_dataset_remote_name(s: String) -> Result<(), String> {
    match RemoteDatasetName::try_from(&s) {
        Ok(_) => Ok(()),
        Err(_) => Err(format!(
            "Remote dataset name should be in form: `repository/dataset-id` or `repository/account/dataset-id`",
        )),
    }
}

fn validate_dataset_ref_local(s: String) -> Result<(), String> {
    match DatasetRefLocal::try_from(&s) {
        Ok(_) => Ok(()),
        Err(_) => Err(format!(
            "Local reference should be in form: `did:odf:...` or `my.dataset.id`",
        )),
    }
}

// fn validate_dataset_ref_remote(s: String) -> Result<(), String> {
//     match DatasetRefRemote::try_from(&s) {
//         Ok(_) => Ok(()),
//         Err(_) => Err(format!(
//             "Remote reference should be in form: `did:odf:...` or `repository/account/dataset-id`",
//         )),
//     }
// }

fn validate_dataset_ref_any(s: String) -> Result<(), String> {
    match DatasetRefAny::try_from(&s) {
        Ok(_) => Ok(()),
        Err(_) => Err(format!(
            "Dataset reference should be in form: `my.dataset.id` or `repository/account/dataset-id` or `did:odf:...`",
        )),
    }
}

fn validate_repository_name(s: String) -> Result<(), String> {
    match RepositoryName::try_from(&s) {
        Ok(_) => Ok(()),
        Err(_) => Err(format!(
            "RepositoryID can only contain alphanumerics, dashes, and dots",
        )),
    }
}

fn validate_log_filter(s: String) -> Result<(), String> {
    let items: Vec<_> = s.split(',').collect();
    for item in items {
        match item {
            "source" => Ok(()),
            "watermark" => Ok(()),
            "data" => Ok(()),
            _ => Err(format!(
                "Filter should be a comma-separated list of values like: source,data,watermark"
            )),
        }?;
    }
    Ok(())
}
