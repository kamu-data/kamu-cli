/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.ingest.prep

import dev.kamu.core.manifests.{PrepStep => PrepStepCfg}
import org.apache.logging.log4j.LogManager

class PrepStepFactory() {
  private val logger = LogManager.getLogger(getClass.getName)

  def getStep(
    config: PrepStepCfg
  ): PrepStep = {
    config match {
      case dc: PrepStepCfg.Decompress =>
        dc.format.toLowerCase match {
          case "gzip" =>
            logger.debug("Extracting gzip")
            new DecompressGZIPStep()
          case "zip" =>
            logger.debug("Extracting zip")
            new DecompressZIPStep(dc)
          case _ =>
            throw new NotImplementedError(
              s"Unknown compression format: ${dc.format}"
            )
        }
      case pipe: PrepStepCfg.Pipe =>
        new ProcessPipeStep(pipe.command)
      case _ =>
        throw new NotImplementedError(s"Unknown prep step: $config")
    }
  }

  def getComposedSteps(
    configs: Seq[PrepStepCfg]
  ): PrepStep = {
    new CompositePrepStep(configs.map(getStep).toVector)
  }

}
