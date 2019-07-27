package dev.kamu.cli.commands

import dev.kamu.cli.{
  AlreadyExistsException,
  MetadataRepository,
  MissingReferenceException
}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager

class AddCommand(
  fileSystem: FileSystem,
  metadataRepository: MetadataRepository,
  manifests: Seq[Path]
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {
    val sources = manifests.map(manifestPath => {
      logger.debug(s"Loading dataset from: $manifestPath")
      metadataRepository.loadDatasetFromFile(manifestPath)
    })

    val numAdded = sources
      .map(ds => {
        try {
          metadataRepository.addDataset(ds)
          true
        } catch {
          case e: AlreadyExistsException =>
            logger.warn(e.getMessage + " - skipping")
            false
          case e: MissingReferenceException =>
            logger.warn(e.getMessage + " - skipping")
            false
        }
      })
      .count(added => added)

    logger.info(s"Added $numAdded datasets")
  }

}
