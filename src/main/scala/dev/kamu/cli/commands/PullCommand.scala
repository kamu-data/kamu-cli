package dev.kamu.cli.commands

import java.io.{BufferedOutputStream, FileOutputStream}
import java.util.zip.{ZipEntry, ZipOutputStream}

import dev.kamu.cli.{MetadataRepository, SparkRunner}
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.utils.fs._
import dev.kamu.core.manifests.{
  DataSourcePolling,
  RepositoryVolumeMap,
  TransformStreaming
}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import yaml.defaults._
import pureconfig.generic.auto._

class PullCommand(
  fileSystem: FileSystem,
  repositoryVolumeMap: RepositoryVolumeMap,
  metadataRepository: MetadataRepository,
  sparkRunner: SparkRunner,
  datasetIDs: Seq[String]
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {
    val sources = datasetIDs.map(metadataRepository.getDataSource)

    val numUpdated = sources
      .map {
        case ds: DataSourcePolling  => pullRoot(ds)
        case ds: TransformStreaming => pullDerivative(ds)
      }
      .count(updated => updated)

    logger.info(s"Done, $numUpdated datasets updated.")
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Root
  ///////////////////////////////////////////////////////////////////////////////////////

  def pullRoot(ds: DataSourcePolling): Boolean = {
    logger.debug(s"Pulling root dataset ${ds.id}")

    val configJar =
      prepareIngestConfigsJar(Seq(ds), repositoryVolumeMap)

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

  def prepareIngestConfigsJar(
    sources: Seq[DataSourcePolling],
    repositoryVolumeMap: RepositoryVolumeMap
  ): Path = {
    val tmpDir = new Path(sys.props("java.io.tmpdir"))
    val configJarPath = tmpDir.resolve("kamu-configs.jar")

    logger.debug(s"Writing temporary configuration JAR to: $configJarPath")

    val fileStream = new BufferedOutputStream(
      new FileOutputStream(configJarPath.toString)
    )
    val zipStream = new ZipOutputStream(fileStream)

    for ((source, i) <- sources.zipWithIndex) {
      zipStream.putNextEntry(new ZipEntry(s"dataSourcePolling_$i.yaml"))
      yaml.save(source.asManifest, zipStream)
      zipStream.closeEntry()
    }

    zipStream.putNextEntry(new ZipEntry("repositoryVolumeMap.yaml"))
    yaml.save(repositoryVolumeMap.asManifest, zipStream)
    zipStream.closeEntry()

    zipStream.close()
    configJarPath
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Derivative
  ///////////////////////////////////////////////////////////////////////////////////////

  def pullDerivative(ds: TransformStreaming): Boolean = {
    logger.debug(s"Running transformations for derivative dataset ${ds.id}")

    val configJar = prepareTransformConfigsJar(Seq(ds), repositoryVolumeMap)

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

  // TODO: Deduplicate code
  def prepareTransformConfigsJar(
    sources: Seq[TransformStreaming],
    repositoryVolumeMap: RepositoryVolumeMap
  ): Path = {
    val tmpDir = new Path(sys.props("java.io.tmpdir"))
    val configJarPath = tmpDir.resolve("kamu-configs.jar")

    logger.debug(s"Writing temporary configuration JAR to: $configJarPath")

    val fileStream = new BufferedOutputStream(
      new FileOutputStream(configJarPath.toString)
    )
    val zipStream = new ZipOutputStream(fileStream)

    for ((source, i) <- sources.zipWithIndex) {
      zipStream.putNextEntry(new ZipEntry(s"transformStreaming_$i.yaml"))
      yaml.save(source.asManifest, zipStream)
      zipStream.closeEntry()
    }

    zipStream.putNextEntry(new ZipEntry("repositoryVolumeMap.yaml"))
    yaml.save(repositoryVolumeMap.asManifest, zipStream)
    zipStream.closeEntry()

    zipStream.close()
    configJarPath
  }
}
