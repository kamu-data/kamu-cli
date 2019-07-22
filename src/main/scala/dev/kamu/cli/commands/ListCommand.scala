package dev.kamu.cli.commands

import dev.kamu.cli.MetadataRepository
import org.apache.log4j.LogManager
import dev.kamu.core.manifests.{DataSourcePolling, TransformStreaming}
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import yaml.defaults._
import pureconfig.generic.auto._

class ListCommand(
  metadataRepository: MetadataRepository
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {

    println("ID, Kind")
    metadataRepository
      .getDataSources()
      .map {
        case ds @ (_: DataSourcePolling)  => (ds.id, "root")
        case ds @ (_: TransformStreaming) => (ds.id, "derivative")
      }
      .sortBy {
        case (id, _) => id
      }
      .foreach {
        case (id, kind) => println(s"$id, $kind")
      }
  }
}
