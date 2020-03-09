/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.output

import java.io.PrintStream

class DelimitedFormatter(
  stream: PrintStream,
  outputFormat: OutputFormat,
  valueFormatter: ValueFormatter
) extends OutputFormatter {

  def format(rs: SimpleResultSet): Unit = {
    if (outputFormat.withHeader)
      printLine(rs.columns)

    rs.rows.foreach(row => printLine(row.map(valueFormatter.format)))
  }

  def printLine(row: Seq[String]): Unit = {
    stream.println(
      row.mkString(outputFormat.delimiter.getOrElse(","))
    )
  }

}
