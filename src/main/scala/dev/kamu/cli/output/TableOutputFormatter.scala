package dev.kamu.cli.output

import java.io.PrintStream
import scala.math.max

class TableOutputFormatter(stream: PrintStream, outputFormat: OutputFormat)
    extends OutputFormatter {

  def format(rs: SimpleResultSet): Unit = {
    val maxColDataWidths = rs.columns.indices
      .map(c => rs.rows.map(row => row(c).toString.length).max)
      .toArray

    val maxColWidths = rs.columns.indices
      .map(c => max(maxColDataWidths(c), rs.columns(c).length))

    if (outputFormat.showHeader)
      writeHeader(rs.columns, maxColWidths)

    rs.rows.foreach(row => {
      writeRow(row.map(_.toString), maxColWidths)
    })

    if (outputFormat.showHeader)
      writeSpacer(maxColWidths)
  }

  def writeHeader(
    values: Seq[String],
    widths: Seq[Int]
  ): Unit = {
    writeSpacer(widths)
    writeRow(values, widths)
    writeSpacer(widths)
  }

  def writeRow(
    values: Seq[String],
    widths: Seq[Int]
  ): Unit = {
    stream.print("| ")
    stream.print(
      values
        .zip(widths)
        .map {
          case (value, width) =>
            value + (" " * (width - value.length))
        }
        .mkString(" | ")
    )
    stream.println(" |")
  }

  def writeSpacer(widths: Seq[Int]): Unit = {
    stream.print("+-")
    stream.print(widths.map("-" * _).mkString("-+-"))
    stream.println("-+")
  }
}
