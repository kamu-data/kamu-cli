/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli

import dev.kamu.cli.output.OutputFormat
import org.apache.hadoop.fs.Path
import org.apache.log4j.Level
import org.rogach.scallop._

///////////////////////////////////////////////////////////////////////////////

class KamuSubcommand(name: String) extends Subcommand(name) {
  override def descr(d: String): Unit = {
    super.descr(d)
    super.banner(d)
  }
}

///////////////////////////////////////////////////////////////////////////////

class TabularOutputSubcommand(name: String) extends KamuSubcommand(name) {
  val outputFormat = opt[String](
    "output-format",
    argName = "format",
    descr = "Format to display the results in. Valid formats are: " +
      "table, vertical, csv, tsv, xmlattrs, xmlelements, json",
    short = 'O'
  )

  val noColor = opt[Boolean](
    "no-color",
    descr = "Control whether color is used for display",
    noshort = true
  )

  val incremental = opt[Boolean](
    "incremental",
    descr = "Display result rows immediately as they are fetched " +
      "(lower latency and memory usage at the price of extra display column padding)",
    noshort = true
  )

  val noHeader = opt[Boolean](
    "no-header",
    descr = "Whether to show column names in query results",
    noshort = true
  )

  val headerInterval = opt[Int](
    "header-interval",
    argName = "int",
    descr = "The number of rows between which headers are displayed",
    noshort = true
  )

  val csvDelimiter = opt[String](
    "csv-delimiter",
    argName = "char",
    descr = "Delimiter in the csv output format",
    noshort = true
  )

  val csvQuoteCharacter = opt[String](
    "csv-quote-character",
    argName = "char",
    descr = "Quote character in the csv output format",
    noshort = true
  )

  val nullValue = opt[String](
    "null-value",
    descr = "Use specified string in place of NULL values",
    noshort = true
  )

  val numberFormat = opt[String](
    "number-format",
    argName = "pattern",
    descr = "Format numbers using DecimalFormat pattern",
    noshort = true
  )

  val dateFormat = opt[String](
    "date-format",
    argName = "pattern",
    descr = "Format dates using SimpleDateFormat pattern",
    noshort = true
  )

  val timeFormat = opt[String](
    "time-format",
    argName = "pattern",
    descr = "Format times using SimpleDateFormat pattern",
    noshort = true
  )

  val timestampFormat = opt[String](
    "timestamp-format",
    argName = "pattern",
    descr = "Format timestamps using SimpleDateFormat pattern",
    noshort = true
  )

  def getOutputFormat: OutputFormat = {
    OutputFormat(
      color = !noColor(),
      incremental = incremental(),
      outputFormat = outputFormat.toOption,
      withHeader = !noHeader(),
      headerInterval = headerInterval.toOption,
      delimiter = csvDelimiter.toOption,
      quoteCharacter = csvQuoteCharacter.toOption,
      nullValue = nullValue.toOption,
      numberFormat = numberFormat.toOption,
      dateFormat = dateFormat.toOption,
      timeFormat = timeFormat.toOption,
      timestampFormat = timestampFormat.toOption
    )
  }
}

///////////////////////////////////////////////////////////////////////////////

class CliArgs(arguments: Seq[String]) extends ScallopConf(arguments) {
  banner(
    "Kamu data management tool" +
      "\nUsage: \033[1mkamu [OPTION] [COMMAND] [OPTION]\033[0m" +
      "\n\nOptions"
  )
  footer(
    "" +
      "\nTo see help of individual subcommands use: \033[1mkamu [COMMAND] -h\033[0m" +
      "\n\nDocumentation is available at https://github.com/kamu-data/kamu-cli"
  )
  shortSubcommandsHelp()

  // converts a string representing either URI or Path to URI
  implicit val _addArgumentConverter = listArgConverter[java.net.URI](
    s =>
      try {
        new java.net.URI(s)
      } catch {
        case _: java.net.URISyntaxException =>
          new java.io.File(s).toPath.toUri
      }
  )

  implicit val _logLevelConverter = singleArgConverter[Level](Level.toLevel)
  implicit val _pathConverter = singleArgConverter[Path](s => new Path(s))
  implicit val _pathListConverter = listArgConverter[Path](s => new Path(s))
  val _envVarConverter = new ValueConverter[Map[String, String]] {
    def resolveEnvVar(s: String): (String, String) = {
      if (s.indexOf("=") < 0)
        (
          s,
          sys.env.getOrElse(
            s,
            throw new UsageException(s"Undefined environment variable: $s")
          )
        )
      else {
        val (left, right) = s.splitAt(s.indexOf("="))
        (left, right.substring(1))
      }
    }

    def parse(s: List[(String, List[String])]) = {
      val l = s.flatMap(_._2).map(resolveEnvVar).toMap
      if (l.isEmpty) Right(None)
      else Right(Some(l))
    }
    val argType = ArgType.LIST
  }

  editBuilder(s => s.copy(helpFormatter = new BetterScallopHelpFormatter()))

  /////////////////////////////////////////////////////////////////////////////

  val debug = opt[Boolean](
    "debug",
    descr = "Enable full debugging output",
    noshort = true
  )

  val logLevel = opt[Level](
    "log-level",
    descr = "Sets logging level",
    noshort = true,
    default = Some(Level.INFO)
  )

  val localSpark = opt[Boolean](
    "local-spark",
    descr = "Use local spark installation",
    noshort = true
  )

  val sparkLogLevel = opt[Level](
    "spark-log-level",
    descr = "Sets logging level for Spark",
    noshort = true,
    default = Some(Level.WARN)
  )

  /////////////////////////////////////////////////////////////////////////////

  val version = new KamuSubcommand("version") {
    descr("Prints the version information of this tool")
  }
  addSubcommand(version)

  /////////////////////////////////////////////////////////////////////////////

  val init = new KamuSubcommand("init") {
    descr("Initialize the workspace in the current directory")

    val pullImages = opt[Boolean](
      "pull-images",
      descr = "Only pull docker images and exit",
      noshort = true
    )
  }
  addSubcommand(init)

  /////////////////////////////////////////////////////////////////////////////

  val list = new TabularOutputSubcommand("list") {
    descr("List all datasets in the workspace")
  }
  addSubcommand(list)

  /////////////////////////////////////////////////////////////////////////////

  val add = new KamuSubcommand("add") {
    descr("Add a new dataset")

    val interactive = opt[Boolean](
      "interactive",
      descr = "Start dataset creation wizard"
    )

    val replace = opt[Boolean](
      "replace",
      descr =
        "Delete and replace the datasets that already exist (data will be lost)"
    )

    val manifests = trailArg[List[java.net.URI]](
      "manifests",
      required = false,
      descr =
        "Manifest URLs, files containing dataset definitions, or remote dataset IDs",
      default = Some(List.empty)
    )
  }
  addSubcommand(add)

  /////////////////////////////////////////////////////////////////////////////

  val purge = new KamuSubcommand("purge") {
    descr("Purge all data of the dataset")

    val all = opt[Boolean](
      "all",
      descr = "Purge all datasets"
    )

    val ids = trailArg[List[String]](
      "ids",
      required = false,
      descr = "IDs of the datasets to purge",
      default = Some(List.empty)
    )
  }
  addSubcommand(purge)

  /////////////////////////////////////////////////////////////////////////////

  val delete = new KamuSubcommand("delete") {
    descr("Delete a dataset")

    val ids = trailArg[List[String]](
      "ids",
      required = false,
      descr = "IDs of the datasets to delete",
      default = Some(List.empty)
    )
  }
  addSubcommand(delete)

  /////////////////////////////////////////////////////////////////////////////

  val pull = new KamuSubcommand("pull") {
    descr("Pull new data for some specific or all datasets")

    val all = opt[Boolean](
      "all",
      descr = "Pull all datasets"
    )

    val recursive = opt[Boolean](
      "recursive",
      descr = "Pull datasets and their dependencies"
    )

    val ids = trailArg[List[String]](
      "ids",
      required = false,
      descr = "IDs of the datasets to pull",
      default = Some(List.empty)
    )
  }
  addSubcommand(pull)

  /////////////////////////////////////////////////////////////////////////////

  val log = new KamuSubcommand("log") {
    descr("Show dataset's metadata records")

    val id = trailArg[String](
      "id",
      required = true,
      descr = "ID of the dataset"
    )
  }
  addSubcommand(log)

  /////////////////////////////////////////////////////////////////////////////

  val push = new KamuSubcommand("push") {
    descr("Push the specified datasets to a remote")

    val remote = opt[String](
      "remote",
      descr = "ID of the remote to push data to",
      required = true
    )

    val all = opt[Boolean](
      "all",
      descr = "Push all datasets"
    )

    val recursive = opt[Boolean](
      "recursive",
      descr = "Push datasets and their dependencies"
    )

    val ids = trailArg[List[String]](
      "ids",
      required = false,
      descr = "IDs of the datasets to push",
      default = Some(List.empty)
    )
  }
  addSubcommand(push)

  /////////////////////////////////////////////////////////////////////////////

  val remote = new KamuSubcommand("remote") {
    descr("List or manipulate the remotes")

    val list = new TabularOutputSubcommand("list") {
      banner("List the existing remotes")
    }
    addSubcommand(list)

    val add = new Subcommand("add") {
      banner("Add a new remote")

      val manifests = trailArg[List[java.net.URI]](
        "manifests",
        required = false,
        descr = "Manifest URLs/files containing remote definitions",
        default = Some(List.empty)
      )

      val replace = opt[Boolean](
        "replace",
        descr = "Delete and replace the remotes that already exist"
      )
    }
    addSubcommand(add)

    val delete = new Subcommand("delete") {
      banner("Delete the existing remote")

      val ids = trailArg[List[String]](
        "ids",
        "IDs of the remotes to delete",
        default = Some(List.empty)
      )
    }
    addSubcommand(delete)
  }
  addSubcommand(remote)

  /////////////////////////////////////////////////////////////////////////////

  val sql = new TabularOutputSubcommand("sql") {
    descr("Executes an SQL query or drops you into an SQL shell")

    val server = new Subcommand("server") {
      banner("Run JDBC server only")

      val port = opt[Int](
        "port",
        descr = "Expose JDBC server on specific port"
      )
    }
    addSubcommand(server)

    val url = opt[java.net.URI](
      "url",
      argName = "url",
      descr =
        "URL to connect the SQL shell to (e.g jdbc:hive2://example.com:10090)",
      noshort = true
    )

    val command = opt[String](
      "command",
      argName = "SQL",
      descr = "SQL command to run"
    )

    val script = opt[Path](
      "script",
      argName = "path",
      descr = "SQL script file to execute",
      noshort = true
    )
  }
  addSubcommand(sql)

  /////////////////////////////////////////////////////////////////////////////

  val notebook = new KamuSubcommand("notebook") {
    descr(
      "Start the Jupyter notebook and Spark to explore the data in the workspace"
    )

    val env = opt[Map[String, String]](
      "env",
      argName = "name|name=value",
      descr =
        "Set or propagate specified environment variable into the notebook",
      default = Some(Map.empty)
    )(_envVarConverter)
  }
  addSubcommand(notebook)

  /////////////////////////////////////////////////////////////////////////////

  val completion = new KamuSubcommand("completion") {
    descr("Outputs auto-completion script")
    footer(
      s"""
      |Installing completions on Linux:
      |
      |If bash-completion is not installed on Linux, please install the 'bash-completion' package
      |via your distribution's package manager. Then load the kamu completion code for bash into
      |the current shell (you can add following into your ~/.bashrc file):
      |
      |    \033[1msource <(kamu completion bash)\033[0m
      """.stripMargin
    )

    val shell = trailArg[String](
      "shell",
      required = true,
      descr = "Type of the shell"
    )
  }
  addSubcommand(completion)

  /////////////////////////////////////////////////////////////////////////////

  val depgraph = new KamuSubcommand("depgraph") {
    descr("Outputs dependency graph of datasets")
    footer(
      "\nYou can visualize it with graphviz by running:" +
        "\n    \033[1mkamu depgraph | dot -Tpng > depgraph.png\033[0m"
    )
  }
  addSubcommand(depgraph)

  /////////////////////////////////////////////////////////////////////////////

  errorMessageHandler = { message =>
    throw new UsageException(message)
  }
  verify()
}

///////////////////////////////////////////////////////////////////////////////

class BetterScallopHelpFormatter extends ScallopHelpFormatter {
  override protected def getShortSubcommandsHelp(s: Scallop): String = {
    val maxCommandLength = s.subbuilders.map(_._1.length).max

    "\n\n" + getSubcommandsSectionName + "\n" +
      s.subbuilders
        .map {
          case (name, option) =>
            s"  \033[1m${name.padTo(maxCommandLength, ' ')}\033[0m   ${option.descr}"
        }
        .mkString("\n")
  }
}

case class EnvVar(
  name: String,
  value: String
)
