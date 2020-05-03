/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.ingest.prep

import java.io.InputStream
import java.util.regex.Pattern

import dev.kamu.core.utils.ZipEntryStream
import dev.kamu.core.manifests.PrepStepKind

class DecompressZIPStep(config: PrepStepKind.Decompress) extends PrepStep {
  override def prepare(inputStream: InputStream): InputStream = {
    val subPathRegex = config.subPathRegex.orElse(
      config.subPath.map(p => Pattern.quote(p.toString))
    )

    val stream = subPathRegex
      .map(
        regex =>
          ZipEntryStream
            .findFirst(inputStream, regex)
            .getOrElse(
              throw new RuntimeException(
                "Failed to find an entry in the Zip file"
              )
            )
      )
      .getOrElse(
        ZipEntryStream.first(inputStream)
      )

    logger.debug(s"Picking Zip entry ${stream.entry.getName}")
    stream
  }
}
