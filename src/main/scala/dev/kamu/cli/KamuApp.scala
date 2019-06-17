package dev.kamu.cli

import java.io.{BufferedOutputStream, FileInputStream, FileOutputStream}
import java.util.zip.{ZipEntry, ZipOutputStream}

import dev.kamu.core.manifests.utils.fs._
import dev.kamu.core.manifests.{
  Manifest,
  DataSourcePolling,
  RepositoryVolumeMap,
  TransformStreaming
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager

import dev.kamu.core.manifests.parsing.pureconfig.yaml
import yaml.defaults._
import pureconfig.generic.auto._

class UsageException(message: String = "", cause: Throwable = None.orNull)
    extends RuntimeException(message, cause)

object KamuApp extends App {
  val logger = LogManager.getLogger(getClass.getName)
  val fileSystem = FileSystem.get(new Configuration())

  val cliParser = new CliParser()
  val cliOptions = cliParser.parse(args)

  val repositoryVolumeMap = RepositoryVolumeMap(
    downloadDir = new Path("./.poll"),
    checkpointDir = new Path("./.checkpoint"),
    dataDirRoot = new Path("./root"),
    dataDirDeriv = new Path("./deriv")
  ).toAbsolute(fileSystem)

  try {
    cliOptions match {
      case Some(c) =>
        if (c.list.isDefined) {
          listDatasets(repositoryVolumeMap)
        } else if (c.ingest.isDefined && c.ingest.get.manifests.nonEmpty) {
          ingestWithManifest(c.ingest.get.manifests, repositoryVolumeMap)
        } else if (c.transform.isDefined && c.transform.get.manifests.nonEmpty) {
          transformWithManifest(c.transform.get.manifests, repositoryVolumeMap)
        } else if (c.notebook.isDefined) {
          startNotebook(repositoryVolumeMap)
        } else {
          println(cliParser.usage())
        }
      case _ =>
    }

  } catch {
    case usg: UsageException =>
      Console.err.println(s"[ERROR] ${usg.getMessage}")
      sys.exit(1)
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // List
  ///////////////////////////////////////////////////////////////////////////////////////

  def listDatasets(repositoryVolumeMap: RepositoryVolumeMap): Unit = {
    val rootDatasets = fileSystem
      .listStatus(repositoryVolumeMap.dataDirRoot)
      .map(_.getPath.getName)

    val derivDatasets = fileSystem
      .listStatus(repositoryVolumeMap.dataDirDeriv)
      .map(_.getPath.getName)

    println("ID, Kind")
    rootDatasets.foreach(ds => println(s"$ds, root"))
    derivDatasets.foreach(ds => println(s"$ds, deriv"))
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Ingest
  ///////////////////////////////////////////////////////////////////////////////////////

  def ingestWithManifest(
    manifests: Seq[Path],
    repositoryVolumeMap: RepositoryVolumeMap
  ): Unit = {
    val sources = manifests.map(manifestPath => {
      logger.debug(s"Loading manifest from: $manifestPath")
      val inputStream = new FileInputStream(manifestPath.toString)
      val ds = yaml.load[Manifest[DataSourcePolling]](inputStream).content
      inputStream.close()
      ds
    })

    val configJar =
      prepareIngestConfigsJar(sources, repositoryVolumeMap)

    try {
      getSparkRunner.submit(
        repo = repositoryVolumeMap,
        appClass = "dev.kamu.core.ingest.polling.IngestApp",
        jars = Seq(configJar)
      )
    } finally {
      fileSystem.delete(configJar, false)
    }

    logger.info("Ingestion completed successfully!")
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
  // Transform
  ///////////////////////////////////////////////////////////////////////////////////////

  def transformWithManifest(
    manifests: Seq[Path],
    repositoryVolumeMap: RepositoryVolumeMap
  ): Unit = {
    val sources = manifests.map(manifestPath => {
      logger.debug(s"Loading manifest from: $manifestPath")
      val inputStream = new FileInputStream(manifestPath.toString)
      val ts = yaml.load[Manifest[TransformStreaming]](inputStream).content
      inputStream.close()
      ts
    })

    val configJar = prepareTransformConfigsJar(sources, repositoryVolumeMap)

    try {
      getSparkRunner.submit(
        repo = repositoryVolumeMap,
        appClass = "dev.kamu.core.transform.streaming.TransformApp",
        jars = Seq(configJar)
      )
    } finally {
      fileSystem.delete(configJar, false)
    }

    logger.info("Transformation completed successfully!")
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

  ///////////////////////////////////////////////////////////////////////////////////////
  // Notebook
  ///////////////////////////////////////////////////////////////////////////////////////

  def startNotebook(repositoryVolumeMap: RepositoryVolumeMap): Unit = {
    val runner = getNotebookRunner
    runner.start()
  }

  ///////////////////////////////////////////////////////////////////////////////////////

  def getSparkRunner: SparkRunner = {
    if (cliOptions.get.useLocalSpark)
      new SparkRunnerLocal(fileSystem)
    else
      new SparkRunnerDocker(fileSystem)
  }

  def getNotebookRunner: NotebookRunnerDocker = {
    new NotebookRunnerDocker(fileSystem)
  }
}
