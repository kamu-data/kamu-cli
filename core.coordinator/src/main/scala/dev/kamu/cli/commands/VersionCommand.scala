/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.commands

class VersionCommand() extends Command {
  override def requiresWorkspace: Boolean = false

  def run(): Unit = {
    println(getClass.getPackage.getImplementationVersion)
  }
}
