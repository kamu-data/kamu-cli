package dev.kamu.cli.output

case class OutputFormat(
  color: Boolean = true,
  incremental: Boolean = false,
  outputFormat: Option[String] = None,
  showHeader: Boolean = true,
  headerInterval: Option[Int] = None,
  delimiter: Option[String] = None,
  quoteCharacter: Option[String] = None,
  nullValue: Option[String] = None,
  numberFormat: Option[String] = None,
  dateFormat: Option[String] = None,
  timeFormat: Option[String] = None,
  timestampFormat: Option[String] = None
)

object OutputFormat {
  val CSV = OutputFormat(
    outputFormat = Some("csv"),
    delimiter = Some(","),
    quoteCharacter = Some("\"")
  )
}

trait OutputFormatter {
  def format(rs: SimpleResultSet): Unit
}
