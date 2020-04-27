/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.ingest.convert

import java.io.InputStream

trait ConversionStep {
  def convert(input: InputStream): InputStream
}

class NoOpConversionStep extends ConversionStep {
  override def convert(input: InputStream): InputStream = input
}
