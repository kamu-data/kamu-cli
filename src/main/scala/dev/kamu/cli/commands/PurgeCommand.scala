package dev.kamu.cli.commands

import dev.kamu.cli.{DoesNotExistsException, MetadataRepository}
import dev.kamu.core.manifests.DatasetID
import org.apache.log4j.LogManager

class PurgeCommand(
  metadataRepository: MetadataRepository,
  ids: Seq[String],
  all: Boolean
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  override def run(): Unit = {
    val toPurge =
      if (all)
        metadataRepository.getAllDatasetIDs()
      else
        ids.map(DatasetID)

    toPurge
      .foreach(id => {
        try {
          logger.info(s"Purging dataset: ${id.toString}")
          metadataRepository.purgeDataset(id)
        } catch {
          case e: DoesNotExistsException =>
            logger.error(e.getMessage)
        }
      })

    logger.info(s"Purged ${toPurge.size} datasets")
  }
}
