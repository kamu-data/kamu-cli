package dev.kamu.cli.commands

import dev.kamu.cli.external.SparkRunner
import dev.kamu.cli.{MetadataRepository, RepositoryVolumeMap}
import dev.kamu.cli.utility.DependencyGraph
import dev.kamu.core.manifests.{Dataset, DatasetID, ExternalSourceFetchUrl}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import yaml.defaults._
import pureconfig.generic.auto._

class PullCommand(
  fileSystem: FileSystem,
  repositoryVolumeMap: RepositoryVolumeMap,
  metadataRepository: MetadataRepository,
  sparkRunner: SparkRunner,
  ids: Seq[String],
  all: Boolean,
  recursive: Boolean
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {
    val datasetIDs = {
      if (all) metadataRepository.getAllDatasetIDs()
      else ids.map(DatasetID)
    }
    val plan = metadataRepository.getDatasetsInDependencyOrder(
      datasetIDs,
      recursive || all // All implies recursive, which is more efficient
    )

    logger.debug(s"Pulling datasets in following order:")
    plan.foreach(d => logger.debug(s"  ${d.id.toString}"))

    // Use Spark to pull each item in the plan
    val numUpdated = plan
      .map(
        ds =>
          ds.kind match {
            case Dataset.Kind.Root       => pullRoot(ds)
            case Dataset.Kind.Derivative => pullDerivative(ds)
          }
      )
      .count(updated => updated)

    logger.info(s"Updated $numUpdated datasets")
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Root
  ///////////////////////////////////////////////////////////////////////////////////////

  def pullRoot(ds: Dataset): Boolean = {
    logger.debug(s"Pulling root dataset ${ds.id}")

    val source = ds.rootPollingSource.get

    val extraMounts = source.fetch match {
      case furl: ExternalSourceFetchUrl =>
        furl.url.getScheme match {
          case "file" | null => List(new Path(furl.url))
          case _             => List.empty
        }
      case _ => List.empty
    }

    sparkRunner.submit(
      repo = repositoryVolumeMap,
      appClass = "dev.kamu.core.ingest.polling.IngestApp",
      extraFiles = Map(
        "repositoryVolumeMap.yaml" -> (
          os => yaml.save(repositoryVolumeMap.toVolumeMap.asManifest, os)
        ),
        "dataset_0.yaml" -> (os => yaml.save(ds.asManifest, os))
      ),
      extraMounts = extraMounts
    )

    logger.debug(s"Successfully pulled root dataset ${ds.id}")

    true
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Derivative
  ///////////////////////////////////////////////////////////////////////////////////////

  def pullDerivative(ds: Dataset): Boolean = {
    logger.debug(s"Running transformations for derivative dataset ${ds.id}")

    sparkRunner.submit(
      repo = repositoryVolumeMap,
      appClass = "dev.kamu.core.transform.streaming.TransformApp",
      extraFiles = Map(
        "repositoryVolumeMap.yaml" -> (
          os => yaml.save(repositoryVolumeMap.toVolumeMap.asManifest, os)
        ),
        "dataset_0.yaml" -> (os => yaml.save(ds.asManifest, os))
      )
    )

    logger.debug(
      s"Successfully applied transformations for derivative dataset ${ds.id}"
    )

    true
  }
}
