package dev.kamu.cli

import java.net.URI

import org.apache.hadoop.fs.Path
import scopt.OParser

case class CliOptions(
  // args
  repository: Option[String] = None,
  // commands
  ingest: Option[IngestOptions] = None,
  transform: Option[TransformOptions] = None
)

case class IngestOptions(
  manifestPath: Option[Path] = None
)

case class TransformOptions(
  manifestPath: Option[Path] = None
)

class CliParser {
  private val builder = OParser.builder[CliOptions]
  private val parser = {
    import builder._
    OParser.sequence(
      programName("kamu"),
      head("Kamu data processing utility"),
      cmd("ingest")
        .text("Create a dataset from an external source")
        .action((_, c) => c.copy(ingest = Some(IngestOptions())))
        .children(
          opt[String]('f', "file")
            .text("Path to a file containing DataSourcePolling manifest")
            .action(
              (v, c) =>
                c.copy(
                  ingest = Some(
                    c.ingest.get
                      .copy(manifestPath = Some(new Path(URI.create(v))))
                  )
                )
            )
        ),
      cmd("transform")
        .text("Run a transformation steps for derivative datasets")
        .action((_, c) => c.copy(transform = Some(TransformOptions())))
        .children(
          opt[String]('f', "file")
            .text("Path to a file containing TransformStreaming manifest")
            .action(
              (v, c) =>
                c.copy(
                  transform = Some(
                    c.transform.get
                      .copy(manifestPath = Some(new Path(URI.create(v))))
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
