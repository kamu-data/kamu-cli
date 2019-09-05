package dev.kamu.cli

import dev.kamu.cli.output._
import dev.kamu.core.manifests.Dataset
import org.apache.hadoop.fs.FileSystem

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
  resultSet: Option[SimpleResultSet]
)

class KamuTestAdapter(
  config: KamuConfig,
  fileSystem: FileSystem
) extends Kamu(config, fileSystem) {

  val _captureFormatter = new CaptureOutputFormatter

  override def getOutputFormatter(
    outputFormat: OutputFormat
  ): OutputFormatter = {
    _captureFormatter
  }

  def runEx(args: String*): CommandResult = {
    super.run(args: _*)
    CommandResult(resultSet = _captureFormatter.resultSet)
  }

  def addDataset(ds: Dataset): Unit = {
    metadataRepository.addDataset(ds)
  }

}
