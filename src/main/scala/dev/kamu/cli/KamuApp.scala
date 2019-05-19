package dev.kamu.cli

import java.net.URI

import dev.kamu.core.manifests.DataSourcePolling
import scopt.OParser

case class Config(
  id: String = "",
  url: URI = null,
  format: String = "",
  action: String = ""
)

object KamuApp extends App {
  val builder = OParser.builder[Config]
  val parser = {
    import builder._
    OParser.sequence(
      programName("kamu"),
      head("Kamu data processing utility"),
      cmd("ingest")
        .text("Create a dataset linked to an external source")
        .action((_, c) => c.copy(action = "ingest"))
        .children(
          opt[String]("id")
            .text("ID of the new dataset")
            .required()
            .action((id, c) => c.copy(id = id)),
          opt[URI]("url")
            .text("Data source URL")
            .required()
            .action((url, c) => c.copy(url = url)),
          opt[String]("format")
            .text("Format")
            .required()
            .action((fmt, c) => c.copy(format = fmt))
        )
    )
  }

  OParser.parse(parser, args, Config()) match {
    case Some(c) =>
      val src = DataSourcePolling(id = c.id, url = c.url, format = c.format)
      println(s"success $src")
    case _ =>
      println("failed")
  }
}
