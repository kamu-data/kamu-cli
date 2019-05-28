package dev.kamu.cli

import scopt.OParser

case class Config(
  // args
  repository: Option[String] = None,
  // commands
  ingest: Option[IngestConfig] = None,
  transform: Option[TransformConfig] = None
)

case class IngestConfig(
  manifestPath: Option[String] = None
)

case class TransformConfig()

object KamuApp extends App {
  val builder = OParser.builder[Config]
  val parser = {
    import builder._
    OParser.sequence(
      programName("kamu"),
      head("Kamu data processing utility"),
      cmd("ingest")
        .text("Create a dataset from an external source")
        .action((_, c) => c.copy(ingest = Some(IngestConfig())))
        .children(
          opt[String]('f', "file")
            .text("Path to a file containing DataSourcePolling manifest")
            .action((v, c) =>
              c.copy(ingest = Some(c.ingest.get.copy(manifestPath = Some(v)))))
        ),
      cmd("transform")
        .text("Run a transformation steps for derivative datasets")
        .action((_, c) => c.copy(transform = Some(TransformConfig())))
        .children(
          opt[String]("id")
            .text("A specific derivative dataset to update")
            .action((v, c) => c)
        )
    )
  }

  OParser.parse(parser, args, Config()) match {
    case Some(c) =>
      if (c.ingest.isDefined) {
        c.ingest.get.manifestPath match {
          case Some(manifestPath) =>
            ingestWithManifest(manifestPath)
          case _ =>
            ingestWithWizard()
        }
      } else {
        println(OParser.usage(parser))
      }
    case _ =>
  }

  def ingestWithManifest(manifestPath: String): Unit = {
    println(s"ingest using manifest $manifestPath")
  }

  def ingestWithWizard(): Unit = {
    println("ingest wizard")
  }
}
