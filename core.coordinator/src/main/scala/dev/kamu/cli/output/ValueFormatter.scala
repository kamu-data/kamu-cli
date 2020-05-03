/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.output

import java.time.{Duration, Instant}

trait FormatHint {
  def get: Any
}

object FormatHint {
  case class MemorySize(v: Long) extends FormatHint {
    override def get: Any = v
  }
  case class RelativeTime(v: Instant) extends FormatHint {
    override def get: Any = v
  }
}

trait ValueFormatter {
  def format(value: Any): String
}

class SimpleValueFormatter extends ValueFormatter {
  override def format(value: Any): String = {
    value match {
      case hint: FormatHint => format(hint.get)
      case other            => other.toString
    }
  }
}

class MissingValueFormatter(placeholder: String, other: ValueFormatter)
    extends ValueFormatter {
  override def format(value: Any): String = {
    value match {
      case null    => placeholder
      case None    => placeholder
      case Some(v) => other.format(v)
      case _       => other.format(value)
    }
  }
}

class ReadablePowerOfTwoValueFormatter(other: ValueFormatter)
    extends ValueFormatter {
  override def format(value: Any): String = {
    value match {
      case FormatHint.MemorySize(size) =>
        if (size < 1024)
          size.toString
        else if (size < 1024 * 1024)
          f"${size / 1024.0}%.1fK"
        else if (size < 1024 * 1024 * 1024)
          f"${(size >> 10) / 1024.0}%.1fM"
        else
          f"${(size >> 20) / 1024.0}%.1fG"
      case _ =>
        other.format(value)
    }
  }
}

class ReadableRelativeTimeValueFormatter(now: Instant, other: ValueFormatter)
    extends ValueFormatter {
  def maybePlural(value: Long, unit: String): String = {
    if (value == 1)
      s"$value $unit ago"
    else
      s"$value ${unit}s ago"
  }
  override def format(value: Any): String = {
    value match {
      case FormatHint.RelativeTime(instant) =>
        val delta = Duration.between(instant, now)
        if (delta.toMillis < 1000 * 60)
          maybePlural(delta.toMillis / 1000, "second")
        else if (delta.toMinutes < 60)
          maybePlural(delta.toMinutes, "minute")
        else if (delta.toHours < 24)
          maybePlural(delta.toHours, "hour")
        else
          maybePlural(delta.toDays, "day")
      case _ =>
        other.format(value)
    }
  }
}
