/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.commands

import dev.kamu.cli.metadata.{DoesNotExistException, MetadataRepository}
import dev.kamu.cli.output.{FormatHint, OutputFormatter, SimpleResultSet}
import org.apache.log4j.LogManager

class ListCommand(
  metadataRepository: MetadataRepository,
  outputFormatter: OutputFormatter
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {

    val rs = new SimpleResultSet()
    rs.addColumn("ID")
    rs.addColumn("Kind")
    rs.addColumn("Records")
    rs.addColumn("Size")
    rs.addColumn("Pulled")

    metadataRepository
      .getAllDatasets()
      .sortBy(_.toString)
      .foreach(id => {
        val kind = metadataRepository.getDatasetKind(id).toString
        try {
          val summary = metadataRepository.getDatasetSummary(id)
          rs.addRow(
            id,
            kind,
            summary.numRecords,
            FormatHint.MemorySize(summary.dataSize),
            summary.lastPulled.map(FormatHint.RelativeTime)
          )
        } catch {
          case _: DoesNotExistException =>
            rs.addRow(
              id,
              kind,
              null,
              null,
              null
            )
        }
      })

    outputFormatter.format(rs)
  }
}
