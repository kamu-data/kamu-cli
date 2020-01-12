/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.commands

import dev.kamu.cli.MetadataRepository
import dev.kamu.cli.output.{OutputFormatter, SimpleResultSet}
import org.apache.log4j.LogManager

class VolumeListCommand(
  metadataRepository: MetadataRepository,
  outputFormatter: OutputFormatter
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {

    val rs = new SimpleResultSet()
    rs.addColumn("ID")
    rs.addColumn("URL")

    metadataRepository
      .getAllVolumes()
      .map(
        v => (v.id.toString, v.url.toString)
      )
      .sortBy {
        case (id, _) => id
      }
      .foreach {
        case (id, url) =>
          rs.addRow(id, url)
      }

    outputFormatter.format(rs)
  }
}
