package dev.kamu.cli.commands

import dev.kamu.cli.{
  AlreadyExistsException,
  MetadataRepository,
  MissingReferenceException,
  SchemaNotSupportedException
}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import java.net.{URI, URISyntaxException}

class AddCommand(
  fileSystem: FileSystem,
  metadataRepository: MetadataRepository,
  manifests: Seq[String]
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {
    val sources = manifests.map(manifestStr => {
      logger.debug(s"Loading dataset from: $manifestStr")
      try {
        val manifestURI = new URI(manifestStr);
        if (null == manifestURI.getScheme)
          metadataRepository.loadDatasetFromFile(new Path(manifestStr))
        else metadataRepository.loadDatasetFromURI(manifestURI)
      } catch {
        case _: URISyntaxException =>
          metadataRepository.loadDatasetFromFile(new Path(manifestStr))
      }
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
          case e: SchemaNotSupportedException =>
            logger.warn(e.getMessage + " - skipping")
            false
        }
      })
      .count(added => added)

    logger.info(s"Added $numAdded datasets")
  }

}
