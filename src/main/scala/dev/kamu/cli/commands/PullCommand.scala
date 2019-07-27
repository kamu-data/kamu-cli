package dev.kamu.cli.commands

import java.io.{BufferedOutputStream, FileOutputStream}
import java.util.zip.{ZipEntry, ZipOutputStream}

import dev.kamu.cli.{MetadataRepository, SparkRunner}
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.utils.fs._
import dev.kamu.core.manifests.{Dataset, DatasetID, RepositoryVolumeMap}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
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
          if (ds.kind == Dataset.Kind.Root)
            pullRoot(ds)
          else
            pullDerivative(ds)
      )
      .count(updated => updated)

    logger.info(s"Done, $numUpdated datasets updated.")
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Root
  ///////////////////////////////////////////////////////////////////////////////////////

  def pullRoot(ds: Dataset): Boolean = {
    logger.debug(s"Pulling root dataset ${ds.id}")

    val configJar =
      prepareConfigsJar(Seq(ds), repositoryVolumeMap)

    try {
      sparkRunner.submit(
        repo = repositoryVolumeMap,
        appClass = "dev.kamu.core.ingest.polling.IngestApp",
        jars = Seq(configJar)
      )
    } finally {
      fileSystem.delete(configJar, false)
    }

    logger.debug(s"Successfully pulled root dataset ${ds.id}")

    true
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Derivative
  ///////////////////////////////////////////////////////////////////////////////////////

  def pullDerivative(ds: Dataset): Boolean = {
    logger.debug(s"Running transformations for derivative dataset ${ds.id}")

    val configJar = prepareConfigsJar(Seq(ds), repositoryVolumeMap)

    try {
      sparkRunner.submit(
        repo = repositoryVolumeMap,
        appClass = "dev.kamu.core.transform.streaming.TransformApp",
        jars = Seq(configJar)
      )
    } finally {
      fileSystem.delete(configJar, false)
    }

    logger.debug(
      s"Successfully applied transformations for derivative dataset ${ds.id}"
    )

    true
  }

  ///////////////////////////////////////////////////////////////////////////////////////

  def prepareConfigsJar(
    datasets: Seq[Dataset],
    repositoryVolumeMap: RepositoryVolumeMap
  ): Path = {
    val tmpDir = new Path(sys.props("java.io.tmpdir"))
    val configJarPath = tmpDir.resolve("kamu-configs.jar")

    logger.debug(s"Writing temporary configuration JAR to: $configJarPath")

    val fileStream = new BufferedOutputStream(
      new FileOutputStream(configJarPath.toString)
    )
    val zipStream = new ZipOutputStream(fileStream)

    for ((ds, i) <- datasets.zipWithIndex) {
      zipStream.putNextEntry(new ZipEntry(s"dataset_$i.yaml"))
      yaml.save(ds.asManifest, zipStream)
      zipStream.closeEntry()
    }

    zipStream.putNextEntry(new ZipEntry("repositoryVolumeMap.yaml"))
    yaml.save(repositoryVolumeMap.asManifest, zipStream)
    zipStream.closeEntry()

    zipStream.close()
    configJarPath
  }
}
