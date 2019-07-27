package dev.kamu.cli

import java.net.URI

import org.apache.hadoop.fs.Path
import scopt.OParser

case class CliOptions(
  // args
  useLocalSpark: Boolean = false,
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

class CliParser {
  private val builder = OParser.builder[CliOptions]
  private val parser = {
    import builder._
    OParser.sequence(
      programName("kamu"),
      head("Kamu data processing utility"),
      help('h', "help").text("prints this usage text"),
      opt[Unit]("local-spark")
        .text("Use local spark installation")
        .action((_, c) => c.copy(useLocalSpark = true)),
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
