package dev.kamu.cli.external

import org.apache.log4j.LogManager

import scala.sys.process.{Process, ProcessBuilder, ProcessIO}

class DockerProcessBuilder(
  protected val id: String,
  protected val dockerClient: DockerClient,
  protected val runArgs: DockerRunArgs
) {
  protected val logger = LogManager.getLogger(getClass.getName)

  def cmd: Seq[String] = {
    dockerClient.makeRunCmd(runArgs)
  }

  def run(): DockerProcess = {
    val processBuilder = dockerClient.prepare(cmd)
    new DockerProcess(
      id,
      dockerClient,
      runArgs.containerName.get,
      processBuilder
    )
  }
}

class DockerProcess(
  id: String,
  dockerClient: DockerClient,
  containerName: String,
  processBuilder: ProcessBuilder
) {
  val process: Process = processBuilder.run(ioHandler())

  protected def ioHandler(): ProcessIO = {
    new ProcessIO(
      _ => (),
      stdout =>
        scala.io.Source
          .fromInputStream(stdout)
          .getLines
          .foreach(l => System.out.println(s"[$id] " + l)),
      stderr =>
        scala.io.Source
          .fromInputStream(stderr)
          .getLines()
          .foreach(l => System.err.println(s"[$id] " + l))
    )
  }

  def join(): Int = {
    process.exitValue()
  }

  def kill(): Unit = {
    dockerClient.kill(containerName)
  }

  def getHostPort(containerPort: Int): Option[Int] = {
    Some(
      dockerClient.inspectHostPort(containerName, s"$containerPort/tcp").toInt
    )
  }
}
