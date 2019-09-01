package dev.kamu.cli.commands

import dev.kamu.cli.external.SparkRunner
import dev.kamu.cli.{MetadataRepository, RepositoryVolumeMap}
import dev.kamu.core.manifests.{Dataset, DatasetID}
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
  datasetIDs: Seq[String],
  all: Boolean
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {
    val datasets =
      if (all)
        metadataRepository.getAllDatasets()
      else
        datasetIDs
          .map(DatasetID)
          .map(metadataRepository.getDataset)

    val numUpdated = datasets
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

    val extraMounts =
      if (source.url.getScheme == null)
        List(new Path(source.url))
      else
        List.empty

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
