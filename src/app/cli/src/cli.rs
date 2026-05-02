// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

use clap::{ArgAction, Parser};
use kamu::domain::ExportFormat;

use crate::cli_value_parser::{self as parsers};
use crate::config::{ConfigObjectFormat, ConfigScope};
use crate::{
    LineageOutputFormat,
    MetadataLogOutputFormat,
    OutputFormat,
    SchemaOutputFormat,
    SqlShellEngine,
    SystemInfoOutputFormat,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Parser)]
#[command(name = crate::BINARY_NAME)]
#[command(version = crate::VERSION)]
#[command(after_help = r#"
To get help for individual commands, use:
    kamu <command> -h
    kamu <command> <sub-command> -h
"#)]
pub struct Cli {
    /// Sets the level of verbosity (repeat for more)
    #[arg(short = 'v', action = ArgAction::Count)]
    pub verbose: u8,

    /// Disable color output in the terminal
    #[arg(long)]
    pub no_color: bool,

    /// Suppress all non-essential output
    #[arg(long, short = 'q')]
    pub quiet: bool,

    /// Do not ask for confirmation and assume the 'yes' answer
    #[arg(long, short = 'y')]
    pub yes: bool,

    /// Record and visualize the command execution as perfetto.dev trace
    #[arg(long)]
    pub trace: bool,

    /// Show stack trace in case of a command execution error
    #[arg(long)]
    pub show_error_stack_trace: bool,

    /// Dump all metrics at the end of command execution
    #[arg(long)]
    pub metrics: bool,

    /// Overrides system time clock with the provided value
    #[arg(long, value_name = "T", hide = true)]
    pub system_time: Option<parsers::DateTimeRfc3339>,

    /// Specifies an account for multi-tenant Workspace
    #[arg(long, short = 'a', hide = true)]
    pub account: Option<String>,

    /// Specifies the hashing mode
    #[arg(long, value_enum, hide = true)]
    pub password_hashing_mode: Option<PasswordHashingMode>,

    /// E2E test interface: file path from which socket bound address will be
    /// read out
    #[arg(long, hide = true)]
    pub e2e_output_data_path: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum PasswordHashingMode {
    Production,
    Testing,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, clap::Subcommand)]
pub enum Command {
    Add(Add),
    Apply(Apply),
    Complete(Complete),
    Completions(Completions),
    Config(Config),
    Context(Context),
    Delete(Delete),
    Export(Export),
    Get(Get),
    Ingest(Ingest),
    Init(Init),
    Inspect(Inspect),
    List(List),
    Log(Log),
    Login(Login),
    Logout(Logout),
    New(New),
    Notebook(Notebook),
    Pull(Pull),
    Push(Push),
    Rename(Rename),
    Reset(Reset),
    Repo(Repo),
    Search(Search),
    Sql(Sql),
    Summary(Summary),
    System(System),
    Tail(Tail),
    Ui(Ui),
    Verify(Verify),
    Version(Version),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Create or update resources from manifest files
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
Applies one or more resource manifests to the active resource context.

If the active context is `local`, manifests are applied to the current
workspace. If the active context points to a remote server, manifests are
applied through the remote GraphQL API.

Use `--dry-run` to preview the accepted changes without applying them.

**Examples:**

Apply a single manifest:

    kamu apply my-resource.yaml

Preview changes without applying them:

    kamu apply my-resource.yaml --dry-run

Apply all manifests in a directory recursively:

    kamu apply manifests/ --recursive

Apply multiple files in the given order:

    kamu apply a.yaml b.json

Apply a manifest from standard input:

    cat my-resource.yaml | kamu apply --stdin

Force JSON parsing regardless of file extension:

    kamu apply generated.resource --format json
"#)]
pub struct Apply {
    #[command(flatten)]
    pub resource_context: ResourceContextArgs,

    /// Manifest file or directory path(s)
    #[arg(value_name = "PATH")]
    pub manifest: Vec<PathBuf>,

    /// Preview the accepted changes without applying them
    #[arg(long)]
    pub dry_run: bool,

    /// Parse all selected files using the specified manifest format
    #[arg(long, value_name = "FMT", value_enum)]
    pub format: Option<ResourceManifestFormat>,

    /// Recursively scan directories for manifests
    #[arg(long, short = 'r')]
    pub recursive: bool,

    /// Read manifest from standard input
    #[arg(long)]
    pub stdin: bool,

    /// Continue processing after per-manifest failures
    #[arg(long)]
    pub continue_on_error: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum ResourceManifestFormat {
    Json,
    Yaml,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Show resource summary for the active context
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
Shows resource counts by kind and reconciliation phase for the active context.

If the active context is `local`, commands target the current workspace. If the
active context points to a remote server, commands target that remote GraphQL
API.

Use `--context` to override the current context for this invocation only.

**Examples:**

Show summary from the active context:

    kamu summary

Show summary from a specific context:

    kamu summary --context prod

Show summary in YAML:

    kamu summary -o yaml
"#)]
pub struct Summary {
    #[command(flatten)]
    pub resource_context: ResourceContextArgs,

    /// Format to display the results in
    #[arg(long, short = 'o', value_name = "FMT", value_enum)]
    pub output_format: Option<SummaryOutputFormat>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum SummaryOutputFormat {
    Table,
    Json,
    Yaml,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, clap::Args)]
pub struct ResourceContextArgs {
    /// Override the current resource context for this invocation
    #[arg(long, short = 'c', value_name = "NAME")]
    pub context: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Cli {
    pub fn tabular_output_format(&self) -> Option<OutputFormat> {
        match &self.command {
            Command::List(c) => c.output_format,
            Command::Context(c) => match &c.subcommand {
                Some(ContextSubCommand::List(sc)) => sc.output_format,
                Some(ContextSubCommand::ApiResources(sc)) => sc.output_format,
                _ => None,
            },
            Command::Repo(c) => match &c.subcommand {
                RepoSubCommand::Alias(sc) => match &sc.subcommand {
                    RepoAliasSubCommand::List(ssc) => ssc.output_format,
                    _ => None,
                },
                RepoSubCommand::List(sc) => sc.output_format,
                _ => None,
            },
            Command::Search(c) => c.output_format,
            Command::Sql(c) => c.output_format,
            Command::Tail(c) => c.output_format,
            _ => None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Manage resource contexts
#[derive(Debug, clap::Args)]
#[command(visible_alias = "ctx")]
#[command(after_help = r#"
Contexts determine which workspace future resource commands will target.

When running inside a workspace, an implicit local context named `local` is
available automatically whenever no current context is selected. You can still
select `local` explicitly. Remote contexts can be registered either in the
workspace or in the user home scope.

**Examples:**

Show current context:

    kamu context

List configured contexts:

    kamu context ls

Check a remote context:

    kamu context check demo

Refresh cached status for all remote contexts:

    kamu context check --all

Switch to a context:

    kamu context prod

Switch back to the local workspace context:

    kamu context local

Register a workspace-scoped remote context:

    kamu context add prod --url https://api.kamu.dev

Register a user-scoped remote context:

    kamu context add prod --url https://api.kamu.dev --user

List supported resource kinds in the active context:

    kamu ctx api-resources

List supported resource kinds from a specific context:

    kamu ctx api-resources --context prod
"#)]
#[command(args_conflicts_with_subcommands = true)]
pub struct Context {
    #[command(subcommand)]
    pub subcommand: Option<ContextSubCommand>,

    /// Context name to switch to
    #[arg()]
    pub name: Option<String>,
}

#[derive(Debug, clap::Subcommand)]
pub enum ContextSubCommand {
    Add(ContextAdd),
    #[command(visible_alias = "ls")]
    List(ContextList),
    #[command(visible_alias = "rm")]
    Remove(ContextRemove),
    Check(ContextCheck),
    ApiResources(ContextApiResources),
    Use(ContextUse),
}

/// List supported resource kinds in the active context
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
Lists resource kinds supported by the active context.

If the active context is `local`, this targets the current workspace. If the
active context points to a remote server, this targets that remote GraphQL API.

Use `--context` to override the current context for this invocation only.

**Examples:**

List supported resource kinds in the active context:

    kamu ctx api-resources

List supported resource kinds from a specific context:

    kamu ctx api-resources --context prod

List supported resource kinds in JSON:

    kamu ctx api-resources -o json
"#)]
pub struct ContextApiResources {
    #[command(flatten)]
    pub resource_context: ResourceContextArgs,

    /// Format to display the results in
    #[arg(long, short = 'o', value_name = "FMT", value_enum)]
    pub output_format: Option<OutputFormat>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Register a new remote resource context
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
Registers a remote workspace context under a local name.

By default the context is stored in the current workspace. Use `--user` to
store it in the user home scope instead.

The name `local` is reserved for the implicit workspace context and cannot be
registered explicitly.

**Examples:**

Add a workspace-scoped remote context:

    kamu context add prod --url https://example.com

Add a user-scoped remote context:

    kamu context add prod --url https://example.com --user
"#)]
pub struct ContextAdd {
    /// Store context in the user home folder rather than in the workspace
    #[arg(long)]
    pub user: bool,

    /// Backend URL of the remote workspace
    #[arg(long, value_name = "URL")]
    pub url: parsers::UrlHttps,

    /// Context name
    #[arg()]
    pub name: String,
}

/// List configured resource contexts
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
Lists effective resource contexts configured in the workspace and user scopes.

When running inside a workspace, the implicit `local` context is also included.

**Examples:**

List contexts:

    kamu context ls

List contexts in JSON:

    kamu context ls -o json
"#)]
pub struct ContextList {
    /// Format to display the results in
    #[arg(long, short = 'o', value_name = "FMT", value_enum)]
    pub output_format: Option<OutputFormat>,
}

/// Remove a remote resource context
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
Removes a previously registered remote context from the selected scope.

By default removal happens in the current workspace. Use `--user` to remove a
user-scoped context instead. Use `--all` to remove all remote contexts from
the selected scope.

The name `local` is reserved and cannot be removed.

**Examples:**

Remove a workspace-scoped context:

    kamu context rm prod

Remove a user-scoped context:

    kamu context rm prod --user

Remove all workspace-scoped remote contexts:

    kamu context rm --all
"#)]
pub struct ContextRemove {
    /// Remove context from the user home folder rather than in the workspace
    #[arg(long)]
    pub user: bool,

    /// Remove all remote contexts in the selected scope
    #[arg(long)]
    pub all: bool,

    /// Context name
    #[arg()]
    pub name: Option<String>,
}

/// Check connectivity and authorization for a remote resource context
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
Checks backend reachability and access token validity for a context.

If no context name is provided, the effective current context is checked.
Use `--all` to refresh cached status for all configured remote contexts.

**Examples:**

Check a named context:

    kamu context check demo

Check the current context:

    kamu context check

Check all remote contexts:

    kamu context check --all
"#)]
pub struct ContextCheck {
    /// Check all effective remote contexts
    #[arg(long)]
    pub all: bool,

    /// Context name
    #[arg()]
    pub name: Option<String>,
}

/// Switch the current resource context
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
Switches the current resource context to the specified named remote context.

This is the explicit form of `kamu context <name>`.

The special name `local` refers to the current workspace when one is available.

**Examples:**

Switch to a context:

    kamu context use prod

Switch to the local workspace context:

    kamu context use local
"#)]
pub struct ContextUse {
    /// Context name
    #[arg()]
    pub name: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Add a new dataset or modify an existing one
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
This command creates a new dataset from the provided DatasetSnapshot manifest.

Note that after kamu creates a dataset the changes in the source file will not have any effect unless you run the add command again. When you are experimenting with adding new dataset you currently may need to delete and re-add it multiple times until you get your parameters and schema right.

In future versions the add command will allow you to modify the structure of already existing datasets (e.g. changing schema in a compatible way).

**Examples:**

Add a root/derivative dataset from local manifest:

    kamu add org.example.data.yaml

Add datasets from all manifests found in the current directory:

    kamu add --recursive .

Add a dataset from manifest hosted externally (e.g. on GihHub):

    kamu add https://raw.githubusercontent.com/kamu-data/kamu-contrib/master/ca.bankofcanada/ca.bankofcanada.exchange-rates.daily.yaml

To add dataset from a repository see `kamu pull` command.
"#)]
pub struct Add {
    /// Recursively search for all manifest in the specified directory
    #[arg(long, short = 'r')]
    pub recursive: bool,

    /// Delete and re-add datasets that already exist
    #[arg(long)]
    pub replace: bool,

    /// Read manifests from standard input
    #[arg(long)]
    pub stdin: bool,

    /// Overrides the name in a loaded manifest
    #[arg(long, value_name = "N")]
    pub name: Option<odf::DatasetAlias>,

    /// Changing the visibility of the added dataset
    #[arg(long, value_name = "VIS", value_enum)]
    pub visibility: Option<parsers::DatasetVisibility>,

    /// Dataset manifest reference(s) (path, or URL)
    #[arg()]
    pub manifest: Vec<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Completes a command in the shell
#[derive(Debug, clap::Args)]
#[command(hide = true)]
#[command(after_help = r#"
This hidden command is called by shell completions to use domain knowledge to complete commands and arguments.

**Examples:**

Should complete to "new":

    kamu complete "kamu ne" 1

Should complete to "--derivative":

    kamu complete "kamu new --de" 2
"#)]
pub struct Complete {
    #[arg(index = 1)]
    pub input: String,

    #[arg(index = 2)]
    pub current: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Generate tab-completion scripts for your shell
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
The command outputs to STDOUT, allowing you to re-direct the output to the file of your choosing. Where you place the file will depend on which shell and which operating system you are using. Your particular configuration may also determine where these scripts need to be placed.

Here are some common set ups:

**Bash:**

Append the following to your `~/.bashrc`:

    source <(kamu completions bash)

You will need to reload your shell session (or execute the same command in your current one) for changes to take effect.

**Zsh:**

Append the following to your `~/.zshrc`:

    autoload -U +X bashcompinit && bashcompinit
    source <(kamu completions bash)

Please contribute a guide for your favorite shell!
"#)]
pub struct Completions {
    #[arg()]
    pub shell: clap_complete::Shell,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Get or set configuration options
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
Configuration in `kamu` is managed very similarly to `git`. Starting with your current workspace and going up the directory tree you can have multiple `.kamuconfig` YAML files which are all merged together to get the resulting config.

Most commonly you will have a workspace-scoped config inside the `.kamu` directory and the user-scoped config residing in your home directory.

**Examples:**

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
"#)]
pub struct Config {
    #[command(subcommand)]
    pub subcommand: ConfigSubCommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum ConfigSubCommand {
    List(ConfigList),
    Get(ConfigGet),
    Set(ConfigSet),
}

/// Display current configuration combined from all config files
#[derive(Debug, clap::Args)]
#[command(visible_alias = "ls")]
pub struct ConfigList {
    /// Which configs to use
    #[arg(long, value_name = "SC", value_enum, default_value_t = ConfigScope::Combined)]
    pub scope: ConfigScope,

    /// Show only user scope configuration
    #[arg(long)]
    pub user: bool,

    /// Show configuration with all default values applied
    #[arg(long)]
    pub with_defaults: bool,

    /// Serialization format of the returned object
    #[arg(long, short = 'o', value_name = "FMT", value_enum, default_value_t = ConfigObjectFormat::Yaml)]
    pub output_format: ConfigObjectFormat,
}

/// Get current configuration value
#[derive(Debug, clap::Args)]
pub struct ConfigGet {
    /// Which configs to use
    #[arg(long, value_name = "SC", value_enum, default_value_t = ConfigScope::Combined)]
    pub scope: ConfigScope,

    /// Operate on the user scope configuration file
    #[arg(long)]
    pub user: bool,

    /// Get default value if config option is not explicitly set
    #[arg(long)]
    pub with_defaults: bool,

    /// Path to the config option
    #[arg()]
    pub cfgkey: Option<String>,

    /// Serialization format of the returned object
    #[arg(long, short = 'o', value_name = "FMT", value_enum, default_value_t = ConfigObjectFormat::Yaml)]
    pub output_format: ConfigObjectFormat,
}

/// Set or unset configuration value
#[derive(Debug, clap::Args)]
pub struct ConfigSet {
    /// Which configs to consider
    #[arg(long, value_name = "SC", value_enum, default_value_t = ConfigScope::Combined)]
    pub scope: ConfigScope,

    /// Operate on the user scope configuration file
    #[arg(long)]
    pub user: bool,

    /// Path to the config option
    #[arg(index = 1)]
    pub cfgkey: String,

    /// New value to set
    #[arg(index = 2)]
    pub value: Option<String>,

    /// Serialization format of the provided object
    #[arg(long, short = 'i', value_name = "FMT", value_enum, default_value_t = ConfigObjectFormat::Yaml)]
    pub input_format: ConfigObjectFormat,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Delete datasets or resources
#[derive(Debug, clap::Args)]
#[command(visible_alias = "rm")]
#[command(after_help = r#"
This command deletes datasets using the legacy dataset path by default, or
resources when a resource target is specified explicitly.

**Examples:**

Delete a local dataset using the legacy default:

    kamu delete my.dataset

Delete datasets explicitly:

    kamu delete datasets my.dataset

Delete local datasets matching a pattern:

    kamu delete my.dataset.%

Delete a single resource:

    kamu delete storages warehouse

Delete all resources of a kind:

    kamu delete storages --all

Delete all resources across kinds:

    kamu delete all

Preview resource deletion:

    kamu delete storages warehouse --dry-run
"#)]
pub struct Delete {
    /// Target to delete: `datasets`, `all`, or a resource selector
    /// such as `variablesets`, `vs`, `secretsets`, `ss`, `storages`, or `st`
    pub target: Option<String>,

    /// Dataset selector(s) in dataset mode, or a single resource selector in
    /// resource mode
    pub args: Vec<String>,

    #[command(flatten)]
    pub resource_context: ResourceContextArgs,

    /// Delete all matched datasets or all resources in the selected scope
    #[arg(long, short = 'a')]
    pub all: bool,

    /// Also delete all transitive dependencies of specified datasets
    #[arg(long, short = 'r')]
    pub recursive: bool,

    /// Do not ask for confirmation
    #[arg(long, short = 'f')]
    pub force: bool,

    /// Exit successfully when a selected resource does not exist
    #[arg(long)]
    pub ignore_not_found: bool,

    /// Preview the resolved resource deletions without deleting anything
    #[arg(long)]
    pub dry_run: bool,

    /// Continue processing resource deletions after per-resource failures
    #[arg(long)]
    pub continue_on_error: bool,
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Exports a dataset
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
This command exports a dataset to a file or set of files of a given format.

Output path may be either file or directory.
When a path contains extention, and no trailing separator, it is considered as a file.
In all other cases a path is considered as a directory. Examples:
 - `export/dataset.csv` is a file path
 - `export/dataset.csv/` is a directory path
 - `export/dataset/` is a directory path
 - `export/dataset` is a directory path
"#)]
pub struct Export {
    /// Local dataset reference
    #[arg(index = 1, value_parser = parsers::dataset_ref)]
    pub dataset: odf::DatasetRef,

    /// Export destination. Dafault is `<current workdir>/<dataset name>`
    #[arg(long)]
    pub output_path: Option<PathBuf>,

    /// Output format
    #[arg(long, value_parser = parsers::export_format)]
    pub output_format: ExportFormat,

    /// Number of records per file, if stored into a directory.
    /// It's a soft limit. For the sake of export performance the actual number
    /// of records may be slightly different.
    #[arg(long)]
    pub records_per_file: Option<usize>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Returns manifest representation of a resource
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
Returns the current state of a single resource as YAML or JSON.

Only real resource kinds supported by the active context are accepted.
Datasets are intentionally not supported by this command.

By default this command returns the full resource view, including status.
Use `--spec` to return the apply-compatible spec manifest instead.

**Examples:**

Get a variable set manifest in YAML:

    kamu get variablesets my-vars

Get the same resource in JSON:

    kamu get vs my-vars -o json

Get the apply-compatible spec manifest:

    kamu get vs my-vars --spec

Get a resource by UUID:

    kamu get variablesets 3d8d6d1c-6f7c-4c62-9f4e-7d8295e8fb69

Read a resource from a remote context:

    kamu get storages warehouse --context prod

Ignore a missing resource:

    kamu get secretsets missing --ignore-not-found
"#)]
pub struct Get {
    /// Resource kind selector such as `variablesets`, `vs`, `secretsets`, or
    /// `ss`
    pub resource: String,

    /// Exact resource name or UUID-v4 resource ID
    pub name_or_id: String,

    #[command(flatten)]
    pub resource_context: ResourceContextArgs,

    /// Serialization format of the returned object
    #[arg(long, short = 'o', value_name = "FMT", value_enum, default_value_t = ResourceManifestFormat::Yaml)]
    pub output_format: ResourceManifestFormat,

    /// Return an apply-compatible spec manifest instead of the full resource
    /// view
    #[arg(long)]
    pub spec: bool,

    /// Exit successfully when the resource does not exist
    #[arg(long)]
    pub ignore_not_found: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Adds data to the root dataset according to its push source configuration
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
**Examples:**

Ingest data from files:

    kamu ingest org.example.data path/to/data.csv

Ingest data from standard input (assumes source is defined to use NDJSON):

    echo '{"key": "value1"}\n{"key": "value2"}' | kamu ingest org.example.data --stdin

Ingest data with format conversion:

    echo '[{"key": "value1"}, {"key": "value2"}]' | kamu ingest org.example.data --stdin --input-format json

Ingest data with event time hint:

    kamu ingest org.example.data data.json --event-time 2050-01-02T12:00:00Z
"#)]
pub struct Ingest {
    /// Name of the push source to use for ingestion
    #[arg(long, value_name = "SRC")]
    pub source_name: Option<String>,

    /// Event time to be used if data does not contain one
    #[arg(long, value_name = "T")]
    pub event_time: Option<String>,

    /// Read data from the standard input
    #[arg(long)]
    pub stdin: bool,

    /// Recursively propagate the updates into all downstream datasets
    #[arg(long, short = 'r')]
    pub recursive: bool,

    /// Overrides the media type of the data expected by the push source
    #[arg(long, value_name = "FMT", value_parser = [
        "csv",
        "json",
        "ndjson",
        "geojson",
        "ndgeojson",
        "parquet",
        "esrishapefile",
    ])]
    pub input_format: Option<String>,

    /// Local dataset reference
    #[arg(index = 1, value_parser = parsers::dataset_ref)]
    pub dataset: odf::DatasetRef,

    /// Data file(s) to ingest
    #[arg(index = 2)]
    pub file: Option<Vec<String>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Initialize an empty workspace in the current directory
#[derive(Debug, Clone, clap::Args)]
#[command(after_help = r#"
A workspace is where kamu stores all the important information about datasets (metadata) and in some cases raw data.

It is recommended to create one kamu workspace per data science project, grouping all related datasets together.

Initializing a workspace creates a `.kamu` directory contains dataset metadata, data, and all supporting files (configs, known repositories etc.).
"#)]
pub struct Init {
    /// Don't return an error if workspace already exists
    #[arg(long)]
    pub exists_ok: bool,

    /// Only pull container images and exit
    #[arg(long)]
    pub pull_images: bool,

    /// List image names instead of pulling
    #[arg(long, hide = true)]
    pub list_only: bool,

    /// Initialize a workspace for multiple tenants
    #[arg(long, hide = true)]
    pub multi_tenant: bool,
}

impl Init {
    pub fn creates_workspace(&self) -> bool {
        !self.pull_images
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Group of commands for exploring dataset metadata
#[derive(Debug, clap::Args)]
pub struct Inspect {
    #[command(subcommand)]
    pub subcommand: InspectSubCommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum InspectSubCommand {
    Lineage(InspectLineage),
    Query(InspectQuery),
    Schema(InspectSchema),
}

/// Shows the dependency tree of a dataset
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
Presents the dataset-level lineage that includes current and past dependencies.

**Examples:**

Show lineage of a single dataset:

    kamu inspect lineage my.dataset

Show lineage graph of all datasets in a browser:

    kamu inspect lineage --browse

Render the lineage graph into a png image (needs graphviz installed):

    kamu inspect lineage -o dot | dot -Tpng > depgraph.png
"#)]
pub struct InspectLineage {
    /// Format of the output
    #[arg(long, short = 'o', value_name = "FMT", value_enum)]
    pub output_format: Option<LineageOutputFormat>,

    /// Produce HTML and open it in a browser
    #[arg(long, short = 'b')]
    pub browse: bool,

    /// Local dataset reference(s)
    #[arg(value_parser = parsers::dataset_ref)]
    pub dataset: Vec<odf::DatasetRef>,
}

/// Shows the transformations used by a derivative dataset
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
This command allows you to audit the transformations performed by a derivative dataset and their evolution. Such audit is an important step in validating the trustworthiness of data (see `kamu verify` command).
"#)]
pub struct InspectQuery {
    /// Local dataset reference
    #[arg(value_parser = parsers::dataset_ref)]
    pub dataset: odf::DatasetRef,
}

/// Shows the dataset schema
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
Displays the schema of the dataset. Note that dataset schemas can evolve over time and by default the latest schema will be shown.

**Examples:**

Show logical schema of a dataset in the DDL format:

    kamu inspect schema my.dataset

Show physical schema of the underlying Parquet files:

    kamu inspect schema my.dataset -o parquet
"#)]
pub struct InspectSchema {
    /// Format of the output
    #[arg(long, short = 'o', value_name = "FMT", value_enum, default_value_t = SchemaOutputFormat::OdfYaml)]
    pub output_format: SchemaOutputFormat,

    /// Extract schema from the last data slice file instead of metadata
    #[arg(long, short = 'b', hide = true)]
    pub from_data_file: bool,

    /// Local dataset reference
    #[arg(value_parser = parsers::dataset_ref)]
    pub dataset: odf::DatasetRef,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// List datasets or resources
#[derive(Debug, clap::Args)]
#[command(visible_alias = "ls")]
#[command(after_help = r#"
**Examples:**

To see a human-friendly list of datasets in your workspace:

    kamu list

To list datasets explicitly:

    kamu list datasets

To list all resources across all kinds:

    kamu list all

To list variable sets:

    kamu list variablesets

To list storages from a specific context:

    kamu list storages --context prod

To see more details:

    kamu list -w

To get a machine-readable list of all resources:

    kamu list all -o csv

To get a machine-readable list of datasets:

    kamu list -o csv
"#)]
pub struct List {
    /// Target to list: `datasets`, `all`, or a resource selector such as
    /// `variablesets`, `vs`, `secretsets`, `ss`, `storages`, or `st`
    pub target: Option<String>,

    #[command(flatten)]
    pub resource_context: ResourceContextArgs,

    /// Format to display the results in
    #[arg(long, short = 'o', value_name = "FMT", value_enum)]
    pub output_format: Option<OutputFormat>,

    /// Show more details (repeat for more)
    #[arg(long, short = 'w', action = ArgAction::Count)]
    pub wide: u8,

    /// List accessible datasets of the specified account
    #[arg(long, hide = true)]
    pub target_account: Option<String>,

    /// List accessible datasets of all accounts
    #[arg(long, hide = true)]
    pub all_accounts: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Shows dataset metadata history
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
Metadata of a dataset contains historical record of everything that ever influenced how data currently looks like.

This includes events such as:
- Data ingestion / transformation
- Change of query
- Change of schema
- Change of source URL or other ingestion steps in a root dataset

Use this command to explore how dataset evolved over time.

**Examples:**

Show brief summaries of individual metadata blocks:

    kamu log org.example.data

Show detailed content of all blocks:

    kamu log -o yaml org.example.data

Using a filter to inspect blocks containing query changes of a derivative dataset:

    kamu log -o yaml --filter source org.example.data
"#)]
pub struct Log {
    /// Format of the output
    #[arg(long, short = 'o', value_name = "FMT", value_enum)]
    pub output_format: Option<MetadataLogOutputFormat>,

    /// Types of events to include
    #[arg(long, short = 'f', value_name = "FLT", value_parser = parsers::log_filter)]
    pub filter: Option<String>,

    /// Maximum number of blocks to display
    #[arg(long, default_value_t = 500)]
    pub limit: usize,

    /// Local dataset reference
    #[arg(value_parser = parsers::dataset_ref)]
    pub dataset: odf::DatasetRef,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Authenticates with a remote ODF server interactively
#[derive(Debug, clap::Args)]
pub struct Login {
    #[command(subcommand)]
    pub subcommand: Option<LoginSubCommand>,

    /// Store access token in the user home folder rather than in the workspace
    #[arg(long)]
    pub user: bool,

    /// Check whether existing authorization is still valid without triggering a
    /// login flow
    #[arg(long)]
    pub check: bool,

    /// Provide an existing access token
    #[arg(long)]
    pub access_token: Option<String>,

    /// ODF server URL (defaults to kamu.dev)
    #[arg()]
    pub server: Option<parsers::UrlHttps>,

    /// Repository name which will be used to store in repositories list
    #[arg(long, value_parser = parsers::repo_name)]
    pub repo_name: Option<odf::RepoName>,

    /// Don't automatically add a remote repository for this host
    #[arg(long)]
    pub skip_add_repo: bool,

    /// Predefined ODF backend URL for E2E testing
    #[arg(long, hide = true)]
    pub predefined_odf_backend_url: Option<parsers::UrlHttps>,
}

#[derive(Debug, clap::Subcommand)]
pub enum LoginSubCommand {
    Oauth(LoginOauth),
    Password(LoginPassword),
}

/// Performs non-interactive login to a remote Kamu server via OAuth provider
/// token
#[derive(Debug, clap::Args)]
pub struct LoginOauth {
    /// Name of the OAuth provider, i.e. 'github'
    #[arg(index = 1)]
    pub provider: String,

    /// OAuth provider access token
    #[arg(index = 2)]
    pub access_token: String,

    /// ODF backend server URL (defaults to kamu.dev)
    #[arg(index = 3)]
    pub server: Option<parsers::UrlHttps>,
}

/// Performs non-interactive login to a remote Kamu server via login and
/// password
#[derive(Debug, clap::Args)]
pub struct LoginPassword {
    /// Specify user name
    #[arg(index = 1)]
    pub login: String,

    /// Specify password
    #[arg(index = 2)]
    pub password: String,

    /// ODF backend server URL (defaults to kamu.dev)
    #[arg(index = 3)]
    pub server: Option<parsers::UrlHttps>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Logs out from a remote Kamu server
#[derive(Debug, clap::Args)]
pub struct Logout {
    /// Drop access token stored in the user home folder rather than in the
    /// workspace
    #[arg(long)]
    pub user: bool,

    /// Log out of all servers
    #[arg(long, short = 'a')]
    pub all: bool,

    /// ODF server URL (defaults to kamu.dev)
    #[arg()]
    pub server: Option<parsers::UrlHttps>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Creates a new dataset manifest from a template
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
This command will create a dataset manifest from a template allowing you to customize the most relevant parts without having to remember the exact structure of the yaml file.

**Examples:**

Create `org.example.data.yaml` file from template in the current directory:

    kamu new org.example.data --root
"#)]
pub struct New {
    /// Create a root dataset
    #[arg(long)]
    pub root: bool,

    /// Create a derivative dataset
    #[arg(long)]
    pub derivative: bool,

    /// Name of the new dataset
    #[arg(value_parser = parsers::dataset_name)]
    pub name: odf::DatasetName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Starts the notebook server for exploring the data in the workspace
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
This command will run the Jupyter server and the Spark engine connected together, letting you query data with SQL before pulling it into the notebook for final processing and visualization.

For more information check out notebook examples at https://github.com/kamu-data/kamu-cli
"#)]
pub struct Notebook {
    /// Expose HTTP server on specific network interface
    #[arg(long)]
    pub address: Option<std::net::IpAddr>,

    /// Expose HTTP server on specific port
    #[arg(long)]
    pub http_port: Option<u16>,

    /// Engine type to use for the notebook
    #[arg(long, value_name = "ENG", value_enum)]
    pub engine: Option<SqlShellEngine>,

    /// Propagate or set an environment variable in the notebook (e.g. `-e VAR`
    /// or `-e VAR=foo`)
    #[arg(long, short = 'e', value_name = "VAR")]
    pub env: Option<Vec<String>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Pull new data into the datasets
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
Pull is a multi-functional command that lets you update a local dataset. Depending on the parameters and the types of datasets involved it can be used to:
- Run polling ingest to pull data into a root dataset from an external source
- Run transformations on a derivative dataset to process previously unseen data
- Pull dataset from a remote repository into your workspace
- Update watermark on a dataset

**Examples:**

Fetch latest data in a specific dataset:

    kamu pull org.example.data

Fetch latest data in datasets matching pattern:

    kamu pull org.example.%

Fetch latest data for the entire dependency tree of a dataset:

    kamu pull --recursive org.example.derivative

Refresh data of all datasets in the workspace:

    kamu pull --all

Fetch dataset from a registered repository:

    kamu pull kamu/org.example.data

Fetch dataset from a URL (see `kamu repo add -h` for supported sources):

    kamu pull ipfs://bafy...a0dx/data
    kamu pull s3://my-bucket.example.org/odf/org.example.data
    kamu pull s3+https://example.org:5000/data --as org.example.data

Advance the watermark of a dataset:

    kamu pull --set-watermark 2020-01-01 org.example.data
"#)]
pub struct Pull {
    /// Pull all datasets in the workspace
    #[arg(long, short = 'a')]
    pub all: bool,

    /// Also pull all transitive dependencies of specified datasets
    #[arg(long, short = 'r')]
    pub recursive: bool,

    /// Pull latest data from uncacheable data sources
    #[arg(long)]
    pub fetch_uncacheable: bool,

    /// Local name of a dataset to use when syncing from a repository
    #[arg(long, value_name = "NAME", value_parser = parsers::dataset_name)]
    pub r#as: Option<odf::DatasetName>,

    /// Don't automatically add a remote push alias for this destination
    #[arg(long)]
    pub no_alias: bool,

    /// Injects a manual watermark into the dataset to signify that no data is
    /// expected to arrive with event time that precedes it
    #[arg(long, value_name = "TIME")]
    pub set_watermark: Option<String>,

    /// Overwrite local version with remote, even if revisions have diverged
    #[arg(long, short = 'f')]
    pub force: bool,

    /// Run hard compaction of derivative dataset if transformation failed due
    /// to root dataset compaction
    #[arg(long)]
    pub reset_derivatives_on_diverged_input: bool,

    /// Local or remote dataset reference(s)
    #[arg(value_parser = parsers::dataset_ref_pattern_any)]
    pub dataset: Option<Vec<odf::DatasetRefAnyPattern>>,

    /// Changing the visibility of the pulled dataset(s)
    #[arg(long, value_name = "VIS", value_enum)]
    pub visibility: Option<parsers::DatasetVisibility>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Push local data into a repository
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
Use this command to share your new dataset or new data with others. All changes performed by this command are atomic and non-destructive. This command will analyze the state of the dataset at the repository and will only upload data and metadata that wasn't previously seen.

Similarly to git, if someone else modified the dataset concurrently with you - your push will be rejected, and you will have to resolve the conflict.

**Examples:**

Sync dataset to a destination URL (see `kamu repo add -h` for supported protocols):

    kamu push org.example.data --to s3://my-bucket.example.org/odf/org.example.data

Sync dataset to a named repository (see `kamu repo` command group):

    kamu push org.example.data --to kamu-hub/org.example.data

Sync dataset that already has a push alias:

    kamu push org.example.data

Sync datasets matching pattern that already have push aliases:

    kamu push org.example.%

Add dataset to local IPFS node and update IPNS entry to the new CID:

    kamu push org.example.data --to ipns://k5..zy
"#)]
pub struct Push {
    /// Push all datasets in the workspace
    #[arg(long, short = 'a')]
    pub all: bool,

    /// Also push all transitive dependencies of specified datasets
    #[arg(long, short = 'r')]
    pub recursive: bool,

    /// Don't automatically add a remote push alias for this destination
    #[arg(long)]
    pub no_alias: bool,

    /// Remote alias or a URL to push to
    #[arg(long, value_name = "REM", value_parser = parsers::dataset_push_target)]
    pub to: Option<odf::DatasetPushTarget>,

    /// Overwrite remote version with local, even if revisions have diverged
    #[arg(long, short = 'f')]
    pub force: bool,

    /// Changing the visibility of the initially pushed dataset(s)
    #[arg(long, value_name = "VIS", value_enum)]
    pub visibility: Option<parsers::DatasetVisibility>,

    /// Local or remote dataset reference(s)
    #[arg(value_parser = parsers::dataset_ref_pattern)]
    pub dataset: Option<Vec<odf::DatasetRefPattern>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Rename a dataset
#[derive(Debug, clap::Args)]
#[command(visible_alias = "mv")]
#[command(after_help = r#"
Use this command to rename a dataset in your local workspace. Renaming is safe in terms of downstream derivative datasets as they use stable dataset IDs to define their inputs.

**Examples:**

Renaming is often useful when you pull a remote dataset by URL, and it gets auto-assigned not the most convenient name:

    kamu pull ipfs://bafy...a0da
    kamu rename bafy...a0da my.dataset
"#)]
pub struct Rename {
    /// Dataset reference
    #[arg(index = 1, value_parser = parsers::dataset_ref)]
    pub dataset: odf::DatasetRef,

    /// The new name to give it
    #[arg(index = 2, value_parser = parsers::dataset_name)]
    pub name: odf::DatasetName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Revert the dataset back to the specified state
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
Resetting a dataset to the specified block erases all metadata blocks that followed it and deletes all data added since that point. This can sometimes be useful to resolve conflicts, but otherwise should be used with care.

Keep in mind that blocks that were pushed to a repository could've been already observed by other people, so resetting the history will not let you take that data back and instead create conflicts for the downstream consumers of your data.
"#)]
pub struct Reset {
    /// Dataset reference
    #[arg(index = 1, value_parser = parsers::dataset_ref)]
    pub dataset: odf::DatasetRef,

    /// Hash of the block to reset to
    #[arg(index = 2, value_parser = parsers::multihash)]
    pub hash: odf::Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Manage set of tracked repositories
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
Repositories are nodes on the network that let users exchange datasets. In the most basic form, a repository can simply be a location where the dataset files are hosted over one of the supported file or object-based data transfer protocols. The owner of a dataset will have push privileges to this location, while other participants can pull data from it.

**Examples:**

Show available repositories:

    kamu repo list

Add S3 bucket as a repository:

    kamu repo add example-repo s3://bucket.my-company.example/
"#)]
pub struct Repo {
    #[command(subcommand)]
    pub subcommand: RepoSubCommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum RepoSubCommand {
    Add(RepoAdd),
    Delete(RepoDelete),
    List(RepoList),
    Alias(RepoAlias),
}

/// Adds a repository
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
For local file system repositories use the following URL formats:

    file:///home/me/example/repository/
    file:///c:/Users/me/example/repository/

For S3-compatible basic repositories use:

    s3://bucket.my-company.example/
    s3+http://my-server:9000/bucket/
    s3+https://my-server:9000/bucket/

For ODF-compatible smart repositories use:

    odf+http://odf-server/
    odf+https://odf-server/
"#)]
pub struct RepoAdd {
    /// Local alias of the repository
    #[arg(index = 1, value_parser = parsers::repo_name)]
    pub name: odf::RepoName,

    /// URL of the repository
    #[arg(index = 2)]
    pub url: url::Url,
}

/// Deletes a reference to repository
#[derive(Debug, clap::Args)]
#[command(visible_alias = "rm")]
pub struct RepoDelete {
    /// Delete all known repositories
    #[arg(long, short = 'a')]
    pub all: bool,

    /// Repository name(s)
    #[arg(value_parser = parsers::repo_name)]
    pub repository: Option<Vec<odf::RepoName>>,
}

/// Lists known repositories
#[derive(Debug, clap::Args)]
#[command(visible_alias = "ls")]
pub struct RepoList {
    /// Format to display the results in
    #[arg(long, short = 'o', value_name = "FMT", value_enum)]
    pub output_format: Option<OutputFormat>,
}

/// Manage set of remote aliases associated with datasets
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
When you pull and push datasets from repositories kamu uses aliases to let you avoid specifying the full remote reference each time. Aliases are usually created the first time you do a push or pull and saved for later. If you have an unusual setup (e.g. pushing to multiple repositories) you can use this command to manage the aliases.

**Examples:**

List all aliases:

    kamu repo alias list

List all aliases of a specific dataset:

    kamu repo alias list org.example.data

Add a new pull alias:

    kamu repo alias add --pull org.example.data kamu.dev/me/org.example.data
"#)]
pub struct RepoAlias {
    #[command(subcommand)]
    pub subcommand: RepoAliasSubCommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum RepoAliasSubCommand {
    Add(RepoAliasAdd),
    Delete(RepoAliasDelete),
    List(RepoAliasList),
}

/// Adds a remote alias to a dataset
#[derive(Debug, clap::Args)]
pub struct RepoAliasAdd {
    /// Add a push alias
    #[arg(long)]
    pub push: bool,

    /// Add a pull alias
    #[arg(long)]
    pub pull: bool,

    /// Local dataset reference
    #[arg(index = 1, value_parser = parsers::dataset_ref)]
    pub dataset: odf::DatasetRef,

    /// Remote dataset name
    #[arg(index = 2, value_parser = parsers::dataset_ref_remote)]
    pub alias: odf::DatasetRefRemote,
}

/// Deletes a remote alias associated with a dataset
#[derive(Debug, clap::Args)]
#[command(visible_alias = "rm")]
pub struct RepoAliasDelete {
    /// Delete all aliases
    #[arg(long, short = 'a')]
    pub all: bool,

    /// Delete a push alias
    #[arg(long)]
    pub push: bool,

    /// Delete a pull alias
    #[arg(long)]
    pub pull: bool,

    /// Local dataset reference
    #[arg(index = 1, value_parser = parsers::dataset_ref)]
    pub dataset: Option<odf::DatasetRef>,

    /// Remote dataset name
    #[arg(index = 2, value_parser = parsers::dataset_ref_remote)]
    pub alias: Option<odf::DatasetRefRemote>,
}

/// Lists remote aliases
#[derive(Debug, clap::Args)]
#[command(visible_alias = "ls")]
pub struct RepoAliasList {
    /// Format to display the results in
    #[arg(long, short = 'o', value_name = "FMT", value_enum)]
    pub output_format: Option<OutputFormat>,

    /// Local dataset reference
    #[arg(value_parser = parsers::dataset_ref)]
    pub dataset: Option<odf::DatasetRef>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Searches for datasets in the registered repositories
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
Search is delegated to the repository implementations and its capabilities depend on the type of the repo. Whereas smart repos may support advanced full-text search, simple storage-only repos may be limited to a substring search by dataset name.

**Examples:**

Search all repositories:

    kamu search covid19

Search only specific repositories:

    kamu search covid19 --repo kamu --repo statcan.gc.ca
"#)]
pub struct Search {
    /// Search local datasets instead of searching in remote repositories
    #[arg(long, short = 'l')]
    pub local: bool,

    /// Maximum results to fetch
    #[arg(long, short = 'n', default_value_t = 10)]
    pub max_results: usize,

    /// Format to display the results in
    #[arg(long, short = 'o', value_name = "FMT", value_enum)]
    pub output_format: Option<OutputFormat>,

    /// Repository name(s) to search in
    #[arg(long, value_parser = parsers::repo_name)]
    pub repo: Option<Vec<odf::RepoName>>,

    /// Search terms
    #[arg()]
    pub query: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Executes an SQL query or drops you into an SQL shell
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
SQL shell allows you to explore data of all dataset in your workspace using one of the supported data processing engines. This can be a great way to prepare and test a query that you cal later turn into derivative dataset.

Output path may be either file or directory.
When a path contains extention, and no trailing separator, it is considered as a file.
In all other cases a path is considered as a directory. Examples:
 - `export/dataset.csv` is a file path
 - `export/dataset.csv/` is a directory path
 - `export/dataset/` is a directory path
 - `export/dataset` is a directory path

**Examples:**

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
"#)]
pub struct Sql {
    /// Format to display the results in
    #[arg(long, short = 'o', value_name = "FMT", value_enum)]
    pub output_format: Option<OutputFormat>,

    #[command(subcommand)]
    pub subcommand: Option<SqlSubCommand>,

    /// Engine type to use for this SQL session
    #[arg(long, value_name = "ENG", value_enum)]
    pub engine: Option<SqlShellEngine>,

    /// URL of a running JDBC server (e.g. jdbc:hive2://example.com:10000)
    #[arg(long)]
    pub url: Option<String>,

    /// SQL command to run
    #[arg(long, short = 'c', value_name = "CMD")]
    pub command: Option<String>,

    /// SQL script file to execute
    #[arg(long, value_name = "FILE")]
    pub script: Option<PathBuf>,

    /// When set, result will be stored to a given path instead of being printed
    /// to stdout.
    #[arg(long)]
    pub output_path: Option<PathBuf>,

    /// Number of records per file, if stored into a directory.
    /// It's a soft limit. For the sake of export performance the actual number
    /// records may be slightly different.
    #[arg(long)]
    pub records_per_file: Option<usize>,
}

#[derive(Debug, clap::Subcommand)]
pub enum SqlSubCommand {
    Server(SqlServer),
}

/// Runs an SQL engine in a server mode
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
**Examples:**

By default runs the DataFusion engine exposing the FlightSQL protocol:

    kamu sql server

To customize interface and port:

    kamu sql server --address 0.0.0.0 --port 50050

To run with Spark engine:

    kamu sql server --engine spark

By default Spark runs with JDBC protocol, to instead run with Livy HTTP gateway:

    kamu sql server --engine spark --livy
"#)]
pub struct SqlServer {
    /// Expose server on specific network interface
    #[arg(long)]
    pub address: Option<std::net::IpAddr>,

    /// Expose server on specific port
    #[arg(long)]
    pub port: Option<u16>,

    /// Engine type to use for this server.
    ///
    /// Currently `datafusion` engine will expose Flight SQL endpoint, while
    /// `spark` engine will expose either JDBC (default) or Livy endpoint (if
    /// `--livy` flag is set).
    #[arg(long, value_name = "ENG", value_enum)]
    pub engine: Option<SqlShellEngine>,

    /// Run Livy server instead of JDBC
    #[arg(long)]
    pub livy: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Command group for system-level functionality
#[derive(Debug, clap::Args)]
pub struct System {
    #[command(subcommand)]
    pub subcommand: SystemSubCommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum SystemSubCommand {
    ApiServer(SystemApiServer),
    Compact(SystemCompact),
    DebugToken(SystemDebugToken),
    Depgraph(SystemDepgraph),
    Decode(SystemDecode),
    Diagnose(SystemDiagnose),
    E2e(SystemE2e),
    GenerateToken(SystemGenerateToken),
    Gc(SystemGc),
    Info(SystemInfo),
    Ipfs(SystemIpfs),
    UpgradeWorkspace(SystemUpgradeWorkspace),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Run HTTP + GraphQL server
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
**Examples:**

Run API server on a specified port:

    kamu system api-server --http-port 12312

Execute a single GraphQL query and print result to stdout:

    kamu system api-server gql-query '{ apiVersion }'

Print out GraphQL API schema:

    kamu system api-server gql-schema
"#)]
pub struct SystemApiServer {
    #[command(subcommand)]
    pub subcommand: Option<SystemApiServerSubCommand>,

    /// Bind to a specific network interface
    #[arg(long)]
    pub address: Option<std::net::IpAddr>,

    /// Expose HTTP+GraphQL server on specific port
    #[arg(long)]
    pub http_port: Option<u16>,

    /// Output a JWT token you can use to authorize API queries
    #[arg(long)]
    pub get_token: bool,

    /// Allows changing the base URL used in the API. Can be handy when
    /// launching inside a container
    #[arg(long)]
    pub external_address: Option<std::net::IpAddr>,
}

#[derive(Debug, clap::Subcommand)]
pub enum SystemApiServerSubCommand {
    GqlQuery(SystemApiServerGqlQuery),
    GqlSchema(SystemApiServerGqlSchema),
}

/// Executes the GraphQL query and prints out the result
#[derive(Debug, clap::Args)]
pub struct SystemApiServerGqlQuery {
    /// Display the full result including extensions
    #[arg(long)]
    pub full: bool,

    /// GQL query
    #[arg()]
    pub query: String,
}

/// Prints the GraphQL schema
#[derive(Debug, clap::Args)]
pub struct SystemApiServerGqlSchema {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Compact a dataset
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
For datasets that get frequent small appends the number of data slices can grow over time and affect the performance of querying. This command allows to merge multiple small data slices into a few large files, which can be beneficial in terms of size from more compact encoding, and in query performance, as data engines will have to scan through far fewer file headers.

There are two types of compactions: soft and hard.

Soft compactions produce new files while leaving the old blocks intact. This allows for faster queries, while still preserving the accurate history of how dataset evolved over time.

Hard compactions rewrite the history of the dataset as if data was originally written in big batches. They allow to shrink the history of a dataset to just a few blocks, reclaim the space used by old data files, but at the expense of history loss. Hard compactions will rewrite the metadata chain, changing block hashes. Therefore, they will **break all downstream datasets** that depend on them.

**Examples:**

Perform a history-altering hard compaction:

    kamu system compact --hard my.dataset
"#)]
pub struct SystemCompact {
    /// Maximum size of a single data slice file in bytes
    #[arg(long, default_value_t = 300000000, value_name = "SIZE")]
    pub max_slice_size: u64,

    /// Maximum amount of records in a single data slice file
    #[arg(long, default_value_t = 10000, value_name = "RECORDS")]
    pub max_slice_records: u64,

    /// Perform 'hard' compaction that rewrites the history of a dataset
    #[arg(long)]
    pub hard: bool,

    /// Perform compaction without saving data blocks
    #[arg(long)]
    pub keep_metadata_only: bool,

    /// Perform verification of the dataset before running a compaction
    #[arg(long)]
    pub verify: bool,

    /// Local dataset references
    #[arg(value_parser = parsers::dataset_ref_pattern)]
    pub dataset: Vec<odf::DatasetRefPattern>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Validate a Kamu token
#[derive(Debug, clap::Args)]
pub struct SystemDebugToken {
    /// Access token
    #[arg()]
    pub token: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Validate a Kamu token
#[derive(Debug, clap::Args)]
pub struct SystemDepgraph {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Decode a manifest file
#[derive(Debug, clap::Args)]
pub struct SystemDecode {
    /// Manifest reference (path, or URL)
    #[arg()]
    pub manifest: Option<String>,

    /// Read manifests from standard input
    #[arg(long)]
    pub stdin: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Run basic system diagnose check
#[derive(Debug, clap::Args)]
pub struct SystemDiagnose {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Hidden command, used exclusively in E2E tests
#[derive(Debug, clap::Args)]
#[command(hide = true)]
pub struct SystemE2e {
    #[arg()]
    pub arguments: Option<Vec<String>>,

    #[arg(long, value_name = "ACT", value_parser = ["get-last-data-block-path", "account-add"])]
    pub action: String,

    /// Local dataset reference
    #[arg(long, value_parser = parsers::dataset_ref)]
    pub dataset: Option<odf::DatasetRef>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs garbage collection to clean up cached and unreachable objects in the
/// workspace
#[derive(Debug, clap::Args)]
pub struct SystemGc {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: This command is temporary and likely will be removed soon
/// Generate a platform token from a known secret for debugging
#[derive(Debug, clap::Args)]
pub struct SystemGenerateToken {
    /// Account ID to generate token for
    #[arg(long)]
    pub subject: Option<String>,

    /// Account name to derive ID from (for predefined accounts only)
    #[arg(long)]
    pub login: Option<String>,

    /// Token expiration time in seconds
    #[arg(long, default_value_t = 3600)]
    pub expiration_time_sec: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Summary of the system information
#[derive(Debug, clap::Args)]
pub struct SystemInfo {
    /// Format of the output
    #[arg(long, short = 'o', value_name = "FMT", value_enum)]
    pub output_format: Option<SystemInfoOutputFormat>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// IPFS helpers
#[derive(Debug, clap::Args)]
pub struct SystemIpfs {
    #[command(subcommand)]
    pub subcommand: SystemIpfsSubCommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum SystemIpfsSubCommand {
    Add(SystemIpfsAdd),
}

/// Adds the specified dataset to IPFS and returns the CID
#[derive(Debug, clap::Args)]
pub struct SystemIpfsAdd {
    /// Dataset reference
    #[arg(value_parser = parsers::dataset_ref)]
    pub dataset: odf::DatasetRef,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Upgrade the layout of a local workspace to the latest version
#[derive(Debug, clap::Args)]
pub struct SystemUpgradeWorkspace {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Displays a sample of most recent records in a dataset
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
This command can be thought of as a shortcut for:

    kamu sql --engine datafusion --command 'select * from "{dataset}" order by {offset_col} desc limit {num_records}'
"#)]
pub struct Tail {
    /// Format to display the results in
    #[arg(long, short = 'o', value_name = "FMT", value_enum)]
    pub output_format: Option<OutputFormat>,

    /// Number of records to display
    #[arg(long, short = 'n', default_value_t = 10, value_name = "NUM")]
    pub num_records: u64,

    /// Number of initial records to skip before applying the limit
    #[arg(long, short = 's', default_value_t = 0, value_name = "SKP")]
    pub skip_records: u64,

    /// Local dataset reference
    #[arg(value_parser = parsers::dataset_ref)]
    pub dataset: odf::DatasetRef,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Opens web interface
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
Starts a built-in HTTP + GraphQL server and opens a pre-packaged Web UI application in your browser.

**Examples:**

Starts server and opens UI in your default browser:

    kamu ui

Start server on a specific port:

    kamu ui --http-port 12345
"#)]
pub struct Ui {
    /// Expose HTTP server on specific network interface
    #[arg(long)]
    pub address: Option<std::net::IpAddr>,

    /// Which port to run HTTP server on
    #[arg(long)]
    pub http_port: Option<u16>,

    /// Output a JWT token you can use to authorize API queries
    #[arg(long)]
    pub get_token: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Verifies the validity of a dataset
#[derive(Debug, clap::Args)]
#[command(after_help = r#"
Validity of derivative data is determined by:
- Trustworthiness of the source data that went into it
- Soundness of the derivative transformation chain that shaped it
- Guaranteeing that derivative data was in fact produced by declared transformations

For the first two, you can inspect the dataset lineage so see which root datasets the data is coming from and whether their publishers are credible. Then you can audit all derivative transformations to ensure they are sound and non-malicious.

This command can help you with the last stage. It uses the history of transformations stored in metadata to first compare the hashes of data with ones stored in metadata (i.e. verify that data corresponds to metadata). Then it repeats all declared transformations locally to ensure that what's declared in metadata actually produces the presented result.

The combination of the above steps can give you a high certainty that the data you're using is trustworthy.

When called on a root dataset the command will only perform the integrity check of comparing data hashes to metadata.

**Examples:**

Verify the data in a dataset starting from its immediate inputs:

    kamu verify com.example.deriv

Verify the data in datasets matching pattern:

    kamu verify com.example.%

Verify the entire transformation chain starting with root datasets (may download a lot of data):

    kamu pull --recursive com.example.deriv

Verify only the hashes of metadata and data, without replaying the transformations. This is useful when you trust the peers performing transformations but want to ensure data was not tampered in storage or during the transmission:

    kamu verify --integrity com.example.deriv
"#)]
pub struct Verify {
    /// Verify the entire transformation chain starting with root datasets
    #[arg(long, short = 'r')]
    pub recursive: bool,

    /// Check only the hashes of metadata and data without replaying
    /// transformations
    #[arg(long)]
    pub integrity: bool,

    /// Local dataset reference(s)
    #[arg(value_parser = parsers::dataset_ref_pattern)]
    pub dataset: Vec<odf::DatasetRefPattern>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Outputs build information
#[derive(Debug, clap::Args)]
pub struct Version {
    /// Format of the output
    #[arg(long, short = 'o', value_name = "FMT", value_enum)]
    pub output_format: Option<SystemInfoOutputFormat>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
