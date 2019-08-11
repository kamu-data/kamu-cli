package dev.kamu.cli

import java.net.URI

import org.apache.hadoop.fs.Path
import org.apache.log4j.Level
import scopt.OParser

case class CliOptions(
  // args
  logLevel: Level = Level.INFO,
  useLocalSpark: Boolean = false,
  sparkLogLevel: Level = Level.WARN,
  repository: Option[String] = None,
  // commands
  init: Option[Unit] = None,
  list: Option[Unit] = None,
  add: Option[AddOptions] = None,
  purge: Option[PurgeOptions] = None,
  delete: Option[DeleteOptions] = None,
  pull: Option[PullOptions] = None,
  // commands - extra
  depgraph: Option[Unit] = None,
  sql: Option[SQLOptions] = None,
  notebook: Option[Unit] = None
)

case class AddOptions(
  manifests: Seq[Path] = Seq.empty,
  interactive: Boolean = false
)

case class PurgeOptions(
  all: Boolean = false,
  ids: Seq[String] = Seq.empty
)

case class DeleteOptions(
  ids: Seq[String] = Seq.empty
)

case class PullOptions(
  all: Boolean = false,
  ids: Seq[String] = Seq.empty
)

case class SQLOptions(
  server: Boolean = false,
  port: Option[Int] = None,
  url: Option[URI] = None,
  command: Option[String] = None,
  script: Option[Path] = None,
  sqlLineOptions: SqlLineOptions = SqlLineOptions()
)

case class SqlLineOptions(
  color: Boolean = true,
  incremental: Option[Boolean] = None,
  outputFormat: Option[String] = None,
  showHeader: Option[Boolean] = None,
  headerInterval: Option[Int] = None,
  csvDelimiter: Option[String] = None,
  csvQuoteCharacter: Option[String] = None,
  nullValue: Option[String] = None,
  numberFormat: Option[String] = None,
  dateFormat: Option[String] = None,
  timeFormat: Option[String] = None,
  timestampFormat: Option[String] = None
)

class CliParser {
  private val builder = OParser.builder[CliOptions]
  private val parser = {
    import builder._
    OParser.sequence(
      programName("kamu"),
      head("Kamu data processing utility"),
      help('h', "help").text("prints this usage text"),
      opt[Unit]("debug")
        .text("Enable full debugging")
        .action(
          (_, c) => c.copy(logLevel = Level.ALL, sparkLogLevel = Level.INFO)
        ),
      opt[String]("log-level")
        .text("Sets logging level")
        .action((lvl, c) => c.copy(logLevel = Level.toLevel(lvl))),
      opt[Unit]("local-spark")
        .text("Use local spark installation")
        .action((_, c) => c.copy(useLocalSpark = true)),
      opt[String]("spark-log-level")
        .text("Sets logging level for Spark")
        .action((lvl, c) => c.copy(sparkLogLevel = Level.toLevel(lvl))),
      cmd("init")
        .text("Initialize the repository in the current directory")
        .action((_, c) => c.copy(init = Some(Nil))),
      cmd("list")
        .text("List all datasets in the repository")
        .action((_, c) => c.copy(list = Some(Nil))),
      cmd("add")
        .text("Add a new dataset")
        .action((_, c) => c.copy(add = Some(AddOptions())))
        .children(
          arg[String]("<manifest>...")
            .text("Paths to the manifest files containing dataset definitions")
            .unbounded()
            .optional()
            .action(
              (x, c) =>
                c.copy(
                  add = Some(
                    c.add.get.copy(
                      manifests = c.add.get.manifests :+ new Path(
                        URI.create(x)
                      )
                    )
                  )
                )
            ),
          opt[Unit]('i', "interactive")
            .text("Start dataset creation wizard")
            .action(
              (_, c) => c.copy(add = Some(c.add.get.copy(interactive = true)))
            )
        ),
      cmd("purge")
        .text("Purge all data of the dataset")
        .action((_, c) => c.copy(purge = Some(PurgeOptions())))
        .children(
          arg[String]("<ID>...")
            .text("IDs of the datasets to purge")
            .unbounded()
            .optional()
            .action(
              (id, c) =>
                c.copy(
                  purge = Some(
                    c.purge.get.copy(
                      ids = c.purge.get.ids :+ id
                    )
                  )
                )
            ),
          opt[Unit]('a', "all")
            .text("Purge all datasets")
            .action(
              (_, c) => c.copy(purge = Some(c.purge.get.copy(all = true)))
            )
        ),
      cmd("delete")
        .text("Delete a dataset")
        .action((_, c) => c.copy(delete = Some(DeleteOptions())))
        .children(
          arg[String]("<ID>...")
            .text("IDs of the datasets to delete")
            .unbounded()
            .required()
            .action(
              (id, c) =>
                c.copy(
                  delete = Some(
                    c.delete.get.copy(
                      ids = c.delete.get.ids :+ id
                    )
                  )
                )
            )
        ),
      cmd("pull")
        .text("Pull new data for some specific or all datasets")
        .action((_, c) => c.copy(pull = Some(PullOptions())))
        .children(
          arg[String]("<manifest>...")
            .text("Path to a files containing TransformStreaming manifests")
            .unbounded()
            .optional()
            .action(
              (id, c) =>
                c.copy(
                  pull = Some(
                    c.pull.get.copy(
                      ids = c.pull.get.ids :+ id
                    )
                  )
                )
            ),
          opt[Unit]('a', "all")
            .text("Pull all datasets")
            .action((_, c) => c.copy(pull = Some(c.pull.get.copy(all = true))))
        ),
      cmd("depgraph")
        .text(
          "Outputs dependency graph of datasets.\n" +
            "You can visualize it with graphviz by running:\n" +
            "  kamu depgraph | dot -Tpng > depgraph.png"
        )
        .action((_, c) => c.copy(depgraph = Some(Nil))),
      cmd("sql")
        .text(
          "Executes an SQL query or drops you into an SQL shell"
        )
        .action((_, c) => c.copy(sql = Some(SQLOptions())))
        .children(
          opt[Unit]("server")
            .text("Run JDBC server only")
            .action(
              (_, c) => c.copy(sql = Some(c.sql.get.copy(server = true)))
            ),
          opt[Int]("port")
            .text("Expose JDBC server on specific port")
            .action(
              (p, c) => c.copy(sql = Some(c.sql.get.copy(port = Some(p))))
            ),
          opt[String]('u', "url")
            .text("URL to connect the SQL shell to")
            .action(
              (url, c) =>
                c.copy(sql = Some(c.sql.get.copy(url = Some(URI.create(url)))))
            ),
          opt[String]('c', "command")
            .text("SQL command to run")
            .action(
              (cmd, c) =>
                c.copy(sql = Some(c.sql.get.copy(command = Some(cmd))))
            ),
          opt[String]("script")
            .text("SQL script file to execute")
            .action(
              (p, c) =>
                c.copy(sql = Some(c.sql.get.copy(script = Some(new Path(p)))))
            ),
          opt[Boolean]("color")
            .text(
              "Control whether color is used for display"
            )
            .action(
              (v, c) =>
                c.copy(
                  sql = Some(
                    c.sql.get.copy(
                      sqlLineOptions = c.sql.get.sqlLineOptions.copy(color = v)
                    )
                  )
                )
            ),
          opt[Boolean]("incremental")
            .text(
              "Display result rows immediately as they are fetched " +
                "(lower latency and memory usage at the price of extra display column padding)"
            )
            .action(
              (i, c) =>
                c.copy(
                  sql = Some(
                    c.sql.get.copy(
                      sqlLineOptions =
                        c.sql.get.sqlLineOptions.copy(incremental = Some(i))
                    )
                  )
                )
            ),
          opt[String]("output-format")
            .text(
              "Format to display the results in (table/vertical/csv/tsv/xmlattrs/xmlelements/json]"
            )
            .action(
              (fmt, c) =>
                c.copy(
                  sql = Some(
                    c.sql.get.copy(
                      sqlLineOptions =
                        c.sql.get.sqlLineOptions.copy(outputFormat = Some(fmt))
                    )
                  )
                )
            ),
          opt[Boolean]("show-header")
            .text(
              "Show column names in query results"
            )
            .action(
              (v, c) =>
                c.copy(
                  sql = Some(
                    c.sql.get.copy(
                      sqlLineOptions =
                        c.sql.get.sqlLineOptions.copy(showHeader = Some(v))
                    )
                  )
                )
            ),
          opt[Int]("header-interval")
            .text(
              "The number of rows between which headers are displayed"
            )
            .action(
              (v, c) =>
                c.copy(
                  sql = Some(
                    c.sql.get.copy(
                      sqlLineOptions =
                        c.sql.get.sqlLineOptions.copy(headerInterval = Some(v))
                    )
                  )
                )
            ),
          opt[String]("csv-delimiter")
            .text("Delimiter in the csv output format")
            .action(
              (v, c) =>
                c.copy(
                  sql = Some(
                    c.sql.get.copy(
                      sqlLineOptions =
                        c.sql.get.sqlLineOptions.copy(csvDelimiter = Some(v))
                    )
                  )
                )
            ),
          opt[String]("csv-quote-character")
            .text("Quote character in the csv output format")
            .action(
              (v, c) =>
                c.copy(
                  sql = Some(
                    c.sql.get.copy(
                      sqlLineOptions = c.sql.get.sqlLineOptions
                        .copy(csvQuoteCharacter = Some(v))
                    )
                  )
                )
            ),
          opt[String]("null-value")
            .text("Use specified string in place of NULL values")
            .action(
              (v, c) =>
                c.copy(
                  sql = Some(
                    c.sql.get.copy(
                      sqlLineOptions =
                        c.sql.get.sqlLineOptions.copy(nullValue = Some(v))
                    )
                  )
                )
            ),
          opt[String]("number-format")
            .text("Format numbers using DecimalFormat pattern")
            .action(
              (v, c) =>
                c.copy(
                  sql = Some(
                    c.sql.get.copy(
                      sqlLineOptions =
                        c.sql.get.sqlLineOptions.copy(numberFormat = Some(v))
                    )
                  )
                )
            ),
          opt[String]("date-format")
            .text("Format dates using SimpleDateFormat pattern")
            .action(
              (v, c) =>
                c.copy(
                  sql = Some(
                    c.sql.get.copy(
                      sqlLineOptions =
                        c.sql.get.sqlLineOptions.copy(dateFormat = Some(v))
                    )
                  )
                )
            ),
          opt[String]("time-format")
            .text("Format times using SimpleDateFormat pattern")
            .action(
              (v, c) =>
                c.copy(
                  sql = Some(
                    c.sql.get.copy(
                      sqlLineOptions =
                        c.sql.get.sqlLineOptions.copy(timeFormat = Some(v))
                    )
                  )
                )
            ),
          opt[String]("timestamp-format")
            .text("Format timestamps using SimpleDateFormat pattern")
            .action(
              (v, c) =>
                c.copy(
                  sql = Some(
                    c.sql.get.copy(
                      sqlLineOptions =
                        c.sql.get.sqlLineOptions.copy(timestampFormat = Some(v))
                    )
                  )
                )
            )
        ),
      cmd("notebook")
        .text(
          "Start the Jupyter notebook server to explore the data in the repository"
        )
        .action((_, c) => c.copy(notebook = Some(Nil)))
    )
  }

  def parse(args: Array[String]): Option[CliOptions] = {
    OParser.parse(parser, args, CliOptions())
  }

  def usage(): String = {
    OParser.usage(parser)
  }
}
