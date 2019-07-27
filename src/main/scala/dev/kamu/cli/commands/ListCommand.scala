package dev.kamu.cli.commands

import dev.kamu.cli.MetadataRepository
import org.apache.log4j.LogManager

class ListCommand(
  metadataRepository: MetadataRepository
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {

    println("ID, Kind")
    metadataRepository
      .getDatasets()
      .map(
        ds => (ds.id, ds.kind.toString)
      )
      .sortBy {
        case (id, _) => id.toString
      }
      .foreach {
        case (id, kind) => println(s"$id, $kind")
      }
  }
}
