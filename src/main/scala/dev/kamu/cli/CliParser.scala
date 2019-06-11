package dev.kamu.cli

import java.net.URI

import org.apache.hadoop.fs.Path
import scopt.OParser

case class CliOptions(
  // args
  useLocalSpark: Boolean = false,
  repository: Option[String] = None,
  // commands
  ingest: Option[IngestOptions] = None,
  transform: Option[TransformOptions] = None
)

case class IngestOptions(
  manifests: Seq[Path] = Seq.empty
)

case class TransformOptions(
  manifests: Seq[Path] = Seq.empty
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
      cmd("ingest")
        .text("Create a dataset from an external source")
        .action((_, c) => c.copy(ingest = Some(IngestOptions())))
        .children(
          arg[String]("<manifest>...")
            .text("Path to a files containing DataSourcePolling manifests")
            .unbounded()
            .optional()
            .action(
              (x, c) =>
                c.copy(
                  ingest = Some(
                    c.ingest.get.copy(
                      manifests = c.ingest.get.manifests :+ new Path(
                        URI.create(x)
                      )
                    )
                  )
                )
            )
        ),
      cmd("transform")
        .text("Run a transformation steps for derivative datasets")
        .action((_, c) => c.copy(transform = Some(TransformOptions())))
        .children(
          arg[String]("<manifest>...")
            .text("Path to a files containing TransformStreaming manifests")
            .unbounded()
            .optional()
            .action(
              (x, c) =>
                c.copy(
                  transform = Some(
                    c.transform.get.copy(
                      manifests = c.transform.get.manifests :+ new Path(
                        URI.create(x)
                      )
                    )
                  )
                )
            )
        )
    )
  }

  def parse(args: Array[String]): Option[CliOptions] = {
    OParser.parse(parser, args, CliOptions())
  }

  def usage(): String = {
    OParser.usage(parser)
  }
}
