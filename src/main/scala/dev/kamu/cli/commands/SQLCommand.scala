package dev.kamu.cli.commands

import dev.kamu.cli.external.{SparkRunner, SparkSQLAppConfig}
import org.apache.log4j.LogManager
import dev.kamu.cli.RepositoryVolumeMap
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import org.apache.hadoop.fs.FileSystem
import yaml.defaults._
import pureconfig.generic.auto._

class SQLCommand(
  fileSystem: FileSystem,
  repositoryVolumeMap: RepositoryVolumeMap,
  sparkRunner: SparkRunner,
  command: Option[String]
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  override def run(): Unit = {
    val config = SparkSQLAppConfig(
      repositoryVolumeMap = repositoryVolumeMap,
      command = command
    )

    sparkRunner.submit(
      repo = repositoryVolumeMap,
      appClass = "dev.kamu.cli.external.SparkSQLApp",
      extraFiles = Map(
        "sqlAppConfig.yaml" -> (os => yaml.save(config, os))
      )
    )
  }
}
