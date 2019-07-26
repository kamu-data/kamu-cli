package dev.kamu.cli.commands

import dev.kamu.cli.{
  DanglingReferenceException,
  DoesNotExistsException,
  MetadataRepository
}
import dev.kamu.core.manifests.DatasetID
import org.apache.log4j.LogManager

class DeleteCommand(
  metadataRepository: MetadataRepository,
  ids: Seq[String]
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  override def run(): Unit = {
    ids
      .map(DatasetID)
      .foreach(id => {
        try {
          metadataRepository.deleteDataSource(id)
          logger.info(s"Deleted dataset: ${id.toString}")
        } catch {
          case e: DoesNotExistsException =>
            logger.error(e.getMessage)
          case e: DanglingReferenceException =>
            logger.error(e.getMessage)
        }
      })
  }
}
