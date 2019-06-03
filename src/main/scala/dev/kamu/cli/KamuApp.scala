package dev.kamu.cli

import java.io.{BufferedOutputStream, FileInputStream, FileOutputStream}
import java.util.zip.{ZipEntry, ZipOutputStream}

import dev.kamu.core.ingest.polling.FSUtils._
import dev.kamu.core.manifests.{DataSourcePolling, RepositoryVolumeMap}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.sys.process.{Process, ProcessIO}

class UsageException(message: String = "", cause: Throwable = None.orNull)
    extends RuntimeException(message, cause)

object KamuApp extends App {
  val fileSystem = FileSystem.get(new Configuration())

  val cliParser = new CliParser()
  val cliOptions = cliParser.parse(args)

  try {
    cliOptions match {
      case Some(c) =>
        if (c.ingest.isDefined) {
          c.ingest.get.manifestPath match {
            case Some(manifestPath) =>
              ingestWithManifest(manifestPath)
            case _ =>
              ingestWithWizard()
          }
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

  def ingestWithManifest(manifestPath: Path): Unit = {
    val inputStream = new FileInputStream(manifestPath.toString)
    val dataSourcePolling = DataSourcePolling.loadManifest(inputStream).content
    inputStream.close()

    val repositoryVolumeMap = RepositoryVolumeMap(
      downloadDir = new Path("./download"),
      checkpointDir = new Path("./checkpoint"),
      dataDir = new Path("./root")
    )

    val configJar =
      prepareIngestConfigsJar(dataSourcePolling, repositoryVolumeMap)

    runIngest(configJar)
  }

  def runIngest(configJar: Path): Unit = {
    val sparkSubmit = findSparkSubmitBin()

    val submitArgs = Seq(
      sparkSubmit.toString,
      "--master=local[4]",
      "--class=dev.kamu.core.ingest.polling.IngestApp",
      s"--jars=${configJar.toString}",
      getAssemblyPath().toString
    )

    val sparkProcess = Process(submitArgs)

    val processIO = new ProcessIO(
      _ => (),
      stdout =>
        scala.io.Source.fromInputStream(stdout).getLines.foreach(println),
      _ => ()
    )

    println("################################################")
    println("# Starting Spark Job")
    println("################################################")
    sparkProcess.!
  }

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

  def prepareIngestConfigsJar(
    dataSourcePolling: DataSourcePolling,
    repositoryVolumeMap: RepositoryVolumeMap
  ): Path = {
    val tmpDir = new Path(sys.props("java.io.tmpdir"))
    val configJarPath = tmpDir.resolve("kamu_configs.jar")

    val fileStream = new BufferedOutputStream(
      new FileOutputStream(configJarPath.toString)
    )
    val zipStream = new ZipOutputStream(fileStream)

    zipStream.putNextEntry(new ZipEntry("dataSourcePolling.yaml"))
    DataSourcePolling.saveManifest(dataSourcePolling.toManifest, zipStream)
    zipStream.closeEntry()

    zipStream.putNextEntry(new ZipEntry("repositoryVolumeMap.yaml"))
    RepositoryVolumeMap.saveManifest(repositoryVolumeMap.toManifest, zipStream)
    zipStream.closeEntry()

    zipStream.close()
    configJarPath
  }

  def getAssemblyPath(): Path = {
    new Path(getClass.getProtectionDomain.getCodeSource.getLocation.toURI)
  }

  def ingestWithWizard(): Unit = {
    println("ingest wizard")
  }
}
