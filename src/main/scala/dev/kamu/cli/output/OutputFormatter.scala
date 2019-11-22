/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.output

case class OutputFormat(
  color: Boolean = true,
  incremental: Boolean = false,
  outputFormat: Option[String] = None,
  withHeader: Boolean = true,
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
    quoteCharacter = Some("\""),
    withHeader = true
  )
}

trait OutputFormatter {
  def format(rs: SimpleResultSet): Unit
}
