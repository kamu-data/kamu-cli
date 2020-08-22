/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.commands

import dev.kamu.cli.metadata.MetadataRepository
import dev.kamu.core.manifests.{DatasetID, DatasetKind}
import org.apache.logging.log4j.LogManager

class DependencyGraphCommand(
  metadataRepository: MetadataRepository
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  def run(): Unit = {
    println("digraph datasets {\nrankdir = LR;")

    def quote(id: DatasetID) = "\"" + id.toString + "\""

    // TODO: Not include dependencies of remote datasets?
    val datasets = metadataRepository
      .getAllDatasets()
      .map(metadataRepository.getDatasetSummary)
      .sortBy(_.id.toString)

    datasets.foreach(
      ds =>
        ds.dependencies
          .foreach(d => println(s"${quote(d)} -> ${quote(ds.id)};"))
    )

    datasets.foreach(
      ds =>
        if (ds.kind == DatasetKind.Root)
          println(s"${quote(ds.id)} [style=filled, fillcolor=darkolivegreen1];")
        else if (ds.kind == DatasetKind.Derivative)
          println(s"${quote(ds.id)} [style=filled, fillcolor=lightblue];")
    )

    println("}")
  }
}
