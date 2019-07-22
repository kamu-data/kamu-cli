package dev.kamu.cli.commands

import dev.kamu.cli.{MetadataRepository, AlreadyExistsException}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import pureconfig.error.ConfigReaderException
import yaml.defaults._
import pureconfig.generic.auto._

class AddManifestCommand(
  fileSystem: FileSystem,
  metadataRepository: MetadataRepository,
  manifests: Seq[Path]
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {
    val sources = manifests.map(manifestPath => {
      logger.debug(s"Loading manifest from: $manifestPath")
      metadataRepository.loadDataSource(manifestPath)
    })

    val numAdded = sources
      .map(ds => {
        try {
          metadataRepository.addDataSource(ds)
          true
        } catch {
          case _: AlreadyExistsException => {
            logger.warn(s"Dataset ${ds.id} already exists, skipping")
            false
          }
        }
      })
      .count(added => added)

    logger.info(s"Added $numAdded datasets")
  }

}
