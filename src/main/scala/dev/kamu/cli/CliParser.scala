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
  pull: Option[PullOptions] = None,
  notebook: Option[Unit] = None
)

case class AddOptions(
  manifests: Seq[Path] = Seq.empty
)

case class PullOptions(
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
