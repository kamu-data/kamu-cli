package dev.kamu.cli.commands

import dev.kamu.cli.{
  AlreadyExistsException,
  MetadataRepository,
  MissingReferenceException
}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager

class AddCommand(
  fileSystem: FileSystem,
  metadataRepository: MetadataRepository,
  manifests: Seq[String]
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {
    val sources = manifests.map(manifestURI => {
      logger.debug(s"Loading dataset from: $manifestURI")
      metadataRepository.loadDatasetFromURI(manifestURI)
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
