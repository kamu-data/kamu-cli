package dev.kamu.cli.commands

import dev.kamu.cli.MetadataRepository
import dev.kamu.core.manifests.{
  DataSourcePolling,
  DatasetID,
  TransformStreaming
}
import org.apache.log4j.LogManager

class DependencyGraphCommand(
  metadataRepository: MetadataRepository
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {
    println("digraph datasets {\nrankdir = LR;")

    def quote(id: DatasetID) = "\"" + id.toString + "\""

    val sources = metadataRepository
      .getDataSources()
      .sortBy(_.id.toString)

    sources.foreach(
      s => s.dependsOn.foreach(d => println(s"${quote(s.id)}  -> ${quote(d)};"))
    )

    sources.foreach {
      case s: DataSourcePolling =>
        println(s"${quote(s.id)} [style=filled, fillcolor=darkolivegreen1];")
      case s: TransformStreaming =>
        println(s"${quote(s.id)} [style=filled, fillcolor=lightblue];")
    }

    println("}")
  }
}
