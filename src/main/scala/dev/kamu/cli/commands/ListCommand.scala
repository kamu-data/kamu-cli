package dev.kamu.cli.commands

import dev.kamu.cli.MetadataRepository
import dev.kamu.cli.output.{OutputFormatter, SimpleResultSet}
import org.apache.log4j.LogManager

class ListCommand(
  metadataRepository: MetadataRepository,
  outputFormatter: OutputFormatter
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {

    val rs = new SimpleResultSet()
    rs.addColumn("ID")
    rs.addColumn("Kind")

    metadataRepository
      .getAllDatasets()
      .map(
        ds => (ds.id, ds.kind.toString)
      )
      .sortBy {
        case (id, _) => id.toString
      }
      .foreach {
        case (id, kind) =>
          rs.addRow(id, kind)
      }

    outputFormatter.format(rs)
  }
}
