/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.ingest.convert

import dev.kamu.core.manifests.ReaderKind
import org.apache.log4j.LogManager

class ConversionStepFactory {
  val logger = LogManager.getLogger(getClass.getName)

  def getStep(readerConfig: ReaderKind): ConversionStep = {
    readerConfig match {
      case _: ReaderKind.Geojson =>
        logger.debug(s"Pre-processing as GeoJSON")
        new GeoJSONConverter()
      case _ =>
        new NoOpConversionStep()
    }
  }

  def getComposedSteps(
    readerConfig: ReaderKind
  ): ConversionStep = {
    getStep(readerConfig)
  }

}
