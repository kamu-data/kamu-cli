/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.commands

import java.io.PrintWriter

import better.files.File
import dev.kamu.cli.{UsageException, WorkspaceLayout}
import org.apache.logging.log4j.LogManager

class InitCommand(
  workspaceLayout: WorkspaceLayout
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  override def requiresWorkspace: Boolean = false

  def run(): Unit = {
    if (File(workspaceLayout.kamuRootDir).isDirectory)
      throw new UsageException("Already a kamu workspace")

    File(workspaceLayout.metadataDir).createDirectories()
    File(workspaceLayout.remotesDir).createDirectories()

    val outputStream = File(workspaceLayout.kamuRootDir.resolve(".gitignore"))
      .newOutputStream()

    val writer = new PrintWriter(outputStream)
    writer.write(WorkspaceLayout.GITIGNORE_CONTENT)
    writer.close()

    logger.info("Initialized an empty workspace")
  }
}
