package dev.kamu.cli

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets

import dev.kamu.cli.output._
import dev.kamu.core.manifests.utils.fs._
import dev.kamu.core.manifests.{Dataset, DatasetID}
import org.apache.hadoop.fs.{FileSystem, Path}
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
  val config: KamuConfig, // config should be public for tests to access repositoryRoot
  fileSystem: FileSystem,
  spark: SparkSession
) extends Kamu(config, fileSystem) {

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

  // TODO: Make sure assembly is compiled and up-to-date, or find some other way to package spark apps for testing
  override def assemblyPath: Path = {
    fileSystem.toAbsolute(new Path("./target/scala-2.11/kamu"))
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

  def addDataset(ds: Dataset): Unit = {
    metadataRepository.addDataset(ds)
  }

  def addDataset(ds: Dataset, df: DataFrame): Unit = {
    metadataRepository.addDataset(ds)
    df.write.parquet(
      repositoryVolumeMap.dataDir.resolve(ds.id.toString).toUri.getPath
    )
  }

  def deleteDataset(id: DatasetID): Unit = {
    metadataRepository.deleteDataset(id)
  }

  def writeData(df: DataFrame, outputFormat: OutputFormat): Path = {
    val name = Random.alphanumeric.take(10).mkString + ".csv"
    val path = config.repositoryRoot.resolve(name)

    df.repartition(1)
      .write
      .option("header", "true")
      .csv(path.toUri.getPath)

    fileSystem
      .listStatus(path)
      .filter(_.getPath.getName.startsWith("part"))
      .head
      .getPath
  }

  def readDataset(id: DatasetID): DataFrame = {
    spark.read.parquet(
      repositoryVolumeMap.dataDir
        .resolve(id.toString)
        .toUri
        .getPath
    )
  }
}
