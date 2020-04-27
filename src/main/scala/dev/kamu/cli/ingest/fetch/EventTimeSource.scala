/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.ingest.fetch

import java.text.SimpleDateFormat
import java.time.Instant

import dev.kamu.core.utils.Clock

trait EventTimeSource {
  def getEventTime(source: FileSystemSource): Option[Instant] =
    getEventTimeDefault()

  def getEventTime(source: AnyRef): Option[Instant] =
    getEventTimeDefault()

  def getEventTimeDefault(): Option[Instant] = None
}

object EventTimeSource {

  class NoEventTime extends EventTimeSource

  class FromSystemTime(systemClock: Clock) extends EventTimeSource {
    override def getEventTimeDefault(): Option[Instant] = {
      Some(systemClock.instant())
    }
  }

  class FromPath(patternStr: String, timestampFormatStr: String)
      extends EventTimeSource {
    val pattern = patternStr.r
    val timestampFormat = new SimpleDateFormat(timestampFormatStr)

    override def getEventTime(source: FileSystemSource): Option[Instant] = {
      pattern.findFirstMatchIn(source.path.toString) match {
        case None =>
          throw new Exception(
            s"Event time pattern didn't match the path: ${source.path}"
          )
        case Some(m) =>
          Some(timestampFormat.parse(m.group(1)).toInstant)
      }

    }
  }

}
