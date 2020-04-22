/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.commands

import java.io.PrintWriter

import dev.kamu.cli.{UsageException, WorkspaceLayout}
import dev.kamu.core.utils.fs._
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager

class InitCommand(
  fileSystem: FileSystem,
  workspaceLayout: WorkspaceLayout
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  override def requiresWorkspace: Boolean = false

  def run(): Unit = {
    if (fileSystem.exists(workspaceLayout.kamuRootDir))
      throw new UsageException("Already a kamu workspace")

    fileSystem.mkdirs(workspaceLayout.metadataDir)
    fileSystem.mkdirs(workspaceLayout.remotesDir)

    val outputStream =
      fileSystem.create(workspaceLayout.kamuRootDir.resolve(".gitignore"))

    val writer = new PrintWriter(outputStream)
    writer.write(WorkspaceLayout.GITIGNORE_CONTENT)
    writer.close()

    logger.info("Initialized an empty workspace")
  }
}
