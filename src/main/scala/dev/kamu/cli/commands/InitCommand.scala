package dev.kamu.cli.commands

import java.io.PrintWriter

import dev.kamu.cli.{UsageException, WorkspaceLayout}
import dev.kamu.core.manifests.utils.fs._
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager

class InitCommand(
  fileSystem: FileSystem,
  workspaceLayout: WorkspaceLayout
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  override def requiresWorkspace: Boolean = false

  def run(): Unit = {
    if (fileSystem.exists(workspaceLayout.metadataRootDir))
      throw new UsageException("Already a kamu workspace")

    fileSystem.mkdirs(workspaceLayout.datasetsDir)

    val outputStream =
      fileSystem.create(workspaceLayout.metadataRootDir.resolve(".gitignore"))

    val writer = new PrintWriter(outputStream)
    writer.write(WorkspaceLayout.GITIGNORE_CONTENT)
    writer.close()

    logger.info("Initialized an empty workspace")
  }
}
