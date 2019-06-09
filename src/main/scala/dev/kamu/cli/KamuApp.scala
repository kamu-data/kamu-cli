package dev.kamu.cli

import java.io.{BufferedOutputStream, FileInputStream, FileOutputStream}
import java.util.zip.{ZipEntry, ZipOutputStream}

import dev.kamu.core.ingest.polling.FSUtils._
import dev.kamu.core.manifests.{
  Manifest,
  DataSourcePolling,
  RepositoryVolumeMap,
  TransformStreaming
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager

import scala.sys.process.{Process, ProcessIO}

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
  )

  try {
    cliOptions match {
      case Some(c) =>
        if (c.ingest.isDefined) {
          c.ingest.get.manifestPath match {
            case Some(manifestPath) =>
              ingestWithManifest(manifestPath, repositoryVolumeMap)
            case _ =>
              ingestWithWizard()
          }
        } else if (c.transform.isDefined) {
          transformWithManifest(
            c.transform.get.manifestPath.get,
            repositoryVolumeMap
          )
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
  // Ingest
  ///////////////////////////////////////////////////////////////////////////////////////

  def ingestWithManifest(
    manifestPath: Path,
    repositoryVolumeMap: RepositoryVolumeMap
  ): Unit = {
    val inputStream = new FileInputStream(manifestPath.toString)
    val dataSourcePolling =
      yaml.load[Manifest[DataSourcePolling]](inputStream).content
    inputStream.close()

    val configJar =
      prepareIngestConfigsJar(dataSourcePolling, repositoryVolumeMap)

    try {
      runIngest(configJar)
    } finally {
      fileSystem.delete(configJar, false)
    }

    logger.info("Ingestion completed successfully!")
  }

  def runIngest(configJar: Path): Unit = {
    val sparkSubmit = findSparkSubmitBin()

    val submitArgs = Seq(
      sparkSubmit.toString,
      "--master=local[4]",
      "--class=dev.kamu.core.ingest.polling.IngestApp",
      s"--jars=${configJar.toString}",
      getAssemblyPath.toString
    )

    logger.debug("Spark cmd: " + submitArgs.mkString(" "))

    val sparkProcess = Process(submitArgs)

    val processIO = new ProcessIO(
      _ => (),
      stdout =>
        scala.io.Source.fromInputStream(stdout).getLines.foreach(println),
      _ => ()
    )

    logger.info("Starting Spark job")
    sparkProcess.!
  }

  def prepareIngestConfigsJar(
    dataSourcePolling: DataSourcePolling,
    repositoryVolumeMap: RepositoryVolumeMap
  ): Path = {
    val tmpDir = new Path(sys.props("java.io.tmpdir"))
    val configJarPath = tmpDir.resolve("kamu_configs.jar")

    logger.debug(s"Writing temporary configuration JAR to: $configJarPath")

    val fileStream = new BufferedOutputStream(
      new FileOutputStream(configJarPath.toString)
    )
    val zipStream = new ZipOutputStream(fileStream)

    zipStream.putNextEntry(new ZipEntry("dataSourcePolling.yaml"))
    yaml.save(dataSourcePolling.asManifest, zipStream)
    zipStream.closeEntry()

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
    manifestPath: Path,
    repositoryVolumeMap: RepositoryVolumeMap
  ): Unit = {
    val inputStream = new FileInputStream(manifestPath.toString)
    val transformStreaming =
      yaml.load[Manifest[TransformStreaming]](inputStream).content
    inputStream.close()

    val configJar =
      prepareTransformConfigsJar(transformStreaming, repositoryVolumeMap)

    try {
      runTransform(configJar)
    } finally {
      fileSystem.delete(configJar, false)
    }

    logger.info("Transformation completed successfully!")
  }

  // TODO: Deduplicate code
  def prepareTransformConfigsJar(
    transformStreaming: TransformStreaming,
    repositoryVolumeMap: RepositoryVolumeMap
  ): Path = {
    val tmpDir = new Path(sys.props("java.io.tmpdir"))
    val configJarPath = tmpDir.resolve("kamu_configs.jar")

    logger.debug(s"Writing temporary configuration JAR to: $configJarPath")

    val fileStream = new BufferedOutputStream(
      new FileOutputStream(configJarPath.toString)
    )
    val zipStream = new ZipOutputStream(fileStream)

    zipStream.putNextEntry(new ZipEntry("transformStreaming.yaml"))
    yaml.save(transformStreaming.asManifest, zipStream)
    zipStream.closeEntry()

    zipStream.putNextEntry(new ZipEntry("repositoryVolumeMap.yaml"))
    yaml.save(repositoryVolumeMap.asManifest, zipStream)
    zipStream.closeEntry()

    zipStream.close()
    configJarPath
  }

  // TODO: Deduplicate code
  def runTransform(configJar: Path): Unit = {
    val sparkSubmit = findSparkSubmitBin()

    val submitArgs = Seq(
      sparkSubmit.toString,
      "--master=local[4]",
      "--class=dev.kamu.core.transform.streaming.TransformApp",
      s"--jars=${configJar.toString}",
      getAssemblyPath.toString
    )

    logger.debug("Spark cmd: " + submitArgs.mkString(" "))

    val sparkProcess = Process(submitArgs)

    val processIO = new ProcessIO(
      _ => (),
      stdout =>
        scala.io.Source.fromInputStream(stdout).getLines.foreach(println),
      _ => ()
    )

    logger.info("Starting Spark job")
    sparkProcess.!
  }

  ///////////////////////////////////////////////////////////////////////////////////////

  def findSparkSubmitBin(): Path = {
    val sparkHome = sys.env.get("SPARK_HOME")
    if (sparkHome.isEmpty)
      throw new UsageException(
        "Can't find $SPARK_HOME environment variable. " +
          "Make sure Spark is installed."
      )

    val sparkSubmit = new Path(sparkHome.get)
      .resolve("bin")
      .resolve("spark-submit")

    if (!fileSystem.isFile(sparkSubmit))
      throw new UsageException(
        s"Can't find spark-submit binary in ${sparkSubmit.toString}"
      )

    sparkSubmit
  }

  def getAssemblyPath: Path = {
    new Path(getClass.getProtectionDomain.getCodeSource.getLocation.toURI)
  }

  def ingestWithWizard(): Unit = {
    println("ingest wizard")
  }
}
