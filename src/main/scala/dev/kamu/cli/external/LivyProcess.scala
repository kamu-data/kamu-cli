package dev.kamu.cli.external

import dev.kamu.cli.RepositoryVolumeMap
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager

import scala.sys.process.{Process, ProcessIO}

class LivyProcess(
  fileSystem: FileSystem,
  repositoryVolumeMap: RepositoryVolumeMap,
  network: String,
  image: String = "kamu/spark-py:2.4.0_0.0.1"
) {
  protected val logger = LogManager.getLogger(getClass.getName)

  val containerName = "kamu-livy"
  val dockerClient = new DockerClient()

  def run(): Process = {
    val dockerArgs = Seq(
      "-p",
      "8998",
      "--hostname",
      containerName,
      "--network",
      network,
      "--name",
      containerName,
      "-v",
      repositoryVolumeMap.dataDir.toUri.getPath + ":/opt/spark/work-dir/data"
    )

    val cmd = dockerClient.makeRunCmd(
      image = image,
      args = Seq("livy"),
      extraArgs = dockerArgs
    )
    val processBuilder = dockerClient.prepare(cmd)
    processBuilder.run(ioHandler)
  }

  def stop(): Unit = {
    dockerClient.kill(containerName)
  }

  def ioHandler: ProcessIO = {
    new ProcessIO(
      _ => (),
      stdout =>
        scala.io.Source
          .fromInputStream(stdout)
          .getLines
          .foreach(l => println("[livy] " + l)),
      stderr =>
        scala.io.Source
          .fromInputStream(stderr)
          .getLines()
          .foreach(l => System.err.println("[livy] " + l))
    )
  }
}
