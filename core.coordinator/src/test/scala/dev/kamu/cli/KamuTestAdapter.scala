/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli

import java.io.{ByteArrayOutputStream, PrintStream, PrintWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.Path

import better.files.File
import dev.kamu.cli.output._
import dev.kamu.core.utils.fs._
import dev.kamu.core.manifests.{DatasetID, DatasetSnapshot}
import dev.kamu.core.utils.ManualClock
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

class CaptureOutputFormatter extends OutputFormatter {
  var resultSet: Option[SimpleResultSet] = None

  override def format(rs: SimpleResultSet): Unit = {
    resultSet = Some(rs)
  }

  def clear() = {
    resultSet = None
  }
}

case class CommandResult(
  resultSet: Option[SimpleResultSet],
  output: String
)

class KamuTestAdapter(
  val config: KamuConfig, // config should be public for tests to access workspaceRoot
  spark: SparkSession,
  val systemClock: ManualClock
) extends Kamu(config, systemClock) {

  val _captureFormatter = new CaptureOutputFormatter
  val _captureOutput = new ByteArrayOutputStream()

  override def getOutputStream(): PrintStream = {
    new PrintStream(_captureOutput, true, "UTF-8")
  }

  override def getOutputFormatter(
    outputFormat: OutputFormat
  ): OutputFormatter = {
    _captureFormatter
  }

  def runEx(args: String*): CommandResult = {
    super.run(args: _*)

    val res = CommandResult(
      resultSet = _captureFormatter.resultSet,
      output = new String(_captureOutput.toByteArray, StandardCharsets.UTF_8)
    )

    _captureFormatter.clear()
    _captureOutput.reset()
    res
  }

  def addDataset(ds: DatasetSnapshot): Unit = {
    metadataRepository.addDataset(ds)
  }

  def addDataset(ds: DatasetSnapshot, df: DataFrame): Unit = {
    metadataRepository.addDataset(ds)
    val volume = metadataRepository.getLocalVolume()

    if (!File(volume.dataDir).exists)
      File(volume.dataDir).createDirectories()

    df.write.parquet(
      volume.dataDir.resolve(ds.id.toString).toUri.getPath
    )
  }

  def deleteDataset(id: DatasetID): Unit = {
    metadataRepository.deleteDataset(id)
  }

  def writeData(content: String, name: String): Path = {
    val path = config.workspaceRoot.resolve(name)

    val writer = new PrintWriter(File(path).newOutputStream)
    writer.write(content)
    writer.close()

    path
  }

  def writeData(df: DataFrame, outputFormat: OutputFormat): Path = {
    val name = Random.alphanumeric.take(10).mkString + ".csv"
    val path = config.workspaceRoot.resolve(name)

    df.repartition(1)
      .write
      .option("header", "true")
      .csv(path.toUri.getPath)

    File(path).list
      .filter(_.name.startsWith("part"))
      .toSeq
      .map(_.path)
      .head
  }

  def readDataset(id: DatasetID): DataFrame = {
    spark.read.parquet(
      metadataRepository
        .getLocalVolume()
        .dataDir
        .resolve(id.toString)
        .toUri
        .getPath
    )
  }
}
