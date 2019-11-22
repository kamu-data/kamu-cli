/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.external

import java.io.OutputStream
import java.util.zip.{ZipEntry, ZipOutputStream}

import dev.kamu.cli.WorkspaceLayout
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
    appClass: String,
    workspaceLayout: WorkspaceLayout,
    extraFiles: Map[String, OutputStream => Unit] = Map.empty,
    extraMounts: Seq[Path] = Seq.empty,
    jars: Seq[Path] = Seq.empty
  ): Unit = {
    val tmpJar =
      if (extraFiles.nonEmpty)
        prepareJar(extraFiles)
      else
        null

    val loggingConfig = prepareLog4jConfig()

    try {
      submit(
        appClass,
        workspaceLayout,
        Seq(tmpJar) ++ jars,
        extraMounts,
        loggingConfig
      )
    } finally {
      if (tmpJar != null)
        fileSystem.delete(tmpJar, false)

      fileSystem.delete(loggingConfig, false)
    }
  }

  protected def submit(
    appClass: String,
    workspaceLayout: WorkspaceLayout,
    jars: Seq[Path],
    extraMounts: Seq[Path],
    loggingConfig: Path
  )

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
