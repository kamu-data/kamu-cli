package dev.kamu.cli.output

import java.io.PrintStream
import scala.math.max

class TableOutputFormat(rs: SimpleResultSet) {

  def format(stream: PrintStream): Unit = {
    val maxColDataWidths = rs.columns.indices
      .map(c => rs.rows.map(row => row(c).toString.length).max)
      .toArray

    val maxColWidths = rs.columns.indices
      .map(c => max(maxColDataWidths(c), rs.columns(c).length))

    writeHeader(rs.columns, maxColWidths, stream)

    rs.rows.foreach(row => {
      writeRow(row.map(_.toString), maxColWidths, stream)
    })

    writeSpacer(maxColWidths, stream)
  }

  def writeHeader(
    values: Seq[String],
    widths: Seq[Int],
    stream: PrintStream
  ): Unit = {
    writeSpacer(widths, stream)
    writeRow(values, widths, stream)
    writeSpacer(widths, stream)
  }

  def writeRow(
    values: Seq[String],
    widths: Seq[Int],
    stream: PrintStream
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

  def writeSpacer(widths: Seq[Int], stream: PrintStream): Unit = {
    stream.print("+-")
    stream.print(widths.map("-" * _).mkString("-+-"))
    stream.println("-+")
  }
}
