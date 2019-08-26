package dev.kamu.cli.external

import java.io.OutputStream
import java.util.zip.{ZipEntry, ZipOutputStream}

import dev.kamu.cli.RepositoryVolumeMap
import dev.kamu.core.manifests.utils.fs._
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, LogManager, Logger}

abstract class SparkRunner(
  fileSystem: FileSystem,
  logLevel: Level
) {
  protected var logger: Logger = LogManager.getLogger(getClass.getName)

  def submit(
    repo: RepositoryVolumeMap,
    appClass: String,
    extraFiles: Map[String, OutputStream => Unit] = Map.empty,
    jars: Seq[Path] = Seq.empty
  ): Unit = {
    val tmpJar =
      if (extraFiles.nonEmpty)
        prepareJar(extraFiles)
      else
        null

    val loggingConfig = prepareLog4jConfig()

    try {
      submit(repo, appClass, Seq(tmpJar) ++ jars, loggingConfig)
    } finally {
      if (tmpJar != null)
        fileSystem.delete(tmpJar, false)

      fileSystem.delete(loggingConfig, false)
    }
  }

  protected def submit(
    repo: RepositoryVolumeMap,
    appClass: String,
    jars: Seq[Path],
    loggingConfig: Path
  )

  protected def assemblyPath: Path = {
    new Path(getClass.getProtectionDomain.getCodeSource.getLocation.toURI)
  }

  protected def prepareJar(files: Map[String, OutputStream => Unit]): Path = {
    val jarPath = tempDir.resolve("kamu-configs.jar")

    logger.debug(s"Writing temporary JAR to: $jarPath")

    val fileStream = fileSystem.create(jarPath, true)
    val zipStream = new ZipOutputStream(fileStream)

    files.foreach {
      case (name, writeFun) =>
        zipStream.putNextEntry(new ZipEntry(name))
        writeFun(zipStream)
        zipStream.closeEntry()
    }

    zipStream.close()
    jarPath
  }

  protected def prepareLog4jConfig(): Path = {
    val path = tempDir.resolve("kamu-spark-log4j.properties")

    val resourceName = logLevel match {
      case Level.ALL | Level.TRACE | Level.DEBUG | Level.INFO =>
        "spark.info.log4j.properties"
      case Level.WARN | Level.ERROR =>
        "spark.warn.log4j.properties"
      case _ =>
        "spark.info.log4j.properties"
    }

    val configStream = getClass.getClassLoader.getResourceAsStream(resourceName)
    val outputStream = fileSystem.create(path, true)

    IOUtils.copy(configStream, outputStream)

    outputStream.close()
    configStream.close()

    path
  }

  protected def tempDir: Path = {
    // Note: not using "java.io.tmpdir" because on Mac this resolves to /var/folders for whatever reason
    // and this directory is not mounted into docker's VM
    val p = fileSystem.toAbsolute(new Path(".kamu/run"))
    if (!fileSystem.exists(p))
      fileSystem.mkdirs(p)
    p
  }
}
