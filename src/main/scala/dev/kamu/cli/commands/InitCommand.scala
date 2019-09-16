package dev.kamu.cli.commands

import java.io.PrintWriter

import dev.kamu.cli.{RepositoryVolumeMap, UsageException}
import dev.kamu.core.manifests.utils.fs._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager

class InitCommand(
  fileSystem: FileSystem,
  repositoryVolumeMap: RepositoryVolumeMap,
  repositoryRoot: Path
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  override def requiresRepository: Boolean = false

  def run(): Unit = {
    if (repositoryVolumeMap.allPaths.exists(fileSystem.exists))
      throw new UsageException("Already a kamu repository")

    repositoryVolumeMap.allPaths.foreach(fileSystem.mkdirs)

    val outputStream = fileSystem.create(repositoryRoot.resolve(".gitignore"))
    val writer = new PrintWriter(outputStream)
    writer.write("""
        |/.kamu/downloads
        |/.kamu/checkpoints
        |/.kamu/data
        |
        |.ipynb_checkpoints
        |""".stripMargin)
    writer.close()

    logger.info("Initialized an empty repository")
  }
}
