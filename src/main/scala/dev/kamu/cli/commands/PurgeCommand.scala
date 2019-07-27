package dev.kamu.cli.commands

import dev.kamu.cli.{DoesNotExistsException, MetadataRepository}
import dev.kamu.core.manifests.DatasetID
import org.apache.log4j.LogManager

class PurgeCommand(
  metadataRepository: MetadataRepository,
  ids: Seq[String]
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  override def run(): Unit = {

    ids
      .map(DatasetID)
      .foreach(id => {
        try {
          metadataRepository.purgeDataset(id)
          logger.info(s"Purged dataset: ${id.toString}")
        } catch {
          case e: DoesNotExistsException =>
            logger.error(e.getMessage)
        }
      })
  }
}
