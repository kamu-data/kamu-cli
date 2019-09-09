package dev.kamu.cli.output

import java.io.PrintStream

class DelimitedFormatter(stream: PrintStream, outputFormat: OutputFormat)
    extends OutputFormatter {

  def format(rs: SimpleResultSet): Unit = {
    if (outputFormat.withHeader)
      printLine(rs.columns)

    rs.rows.foreach(row => printLine(row.map(_.toString)))
  }

  def printLine(row: Seq[String]): Unit = {
    stream.println(
      row.mkString(outputFormat.delimiter.getOrElse(","))
    )
  }

}
