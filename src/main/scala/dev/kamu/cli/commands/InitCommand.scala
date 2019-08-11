package dev.kamu.cli.commands

import java.io.PrintWriter

import org.apache.log4j.LogManager
import dev.kamu.cli.{RepositoryVolumeMap, UsageException}
import org.apache.hadoop.fs.{FileSystem, Path}

class InitCommand(
  fileSystem: FileSystem,
  repositoryVolumeMap: RepositoryVolumeMap
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  override def requiresRepository: Boolean = false

  def run(): Unit = {
    if (repositoryVolumeMap.allPaths.exists(fileSystem.exists))
      throw new UsageException("Already a kamu repository")

    repositoryVolumeMap.allPaths.foreach(fileSystem.mkdirs)
    logger.info("Initialized an empty repository")

    val outputStream = fileSystem.create(new Path(".gitignore"))
    val writer = new PrintWriter(outputStream)
    writer.write("""
        |/.kamu/downloads
        |/.kamu/checkpoints
        |/.kamu/data
        |
        |.ipynb_checkpoints
      """.stripMargin)
    writer.close()
  }
}
