package dev.kamu.cli

import org.apache.log4j.LogManager

import scala.sys.process.{Process, ProcessBuilder}

class DockerClient {
  protected val logger = LogManager.getLogger(getClass.getName)

  def run(
    image: String,
    args: Seq[String] = Seq.empty,
    extraArgs: Seq[String] = Seq.empty
  ): Unit = {
    val cmd = makeRunCmd(image, args, extraArgs)
    val process = prepare(cmd)
    val exitCode = process.!
    if (exitCode != 0)
      throw new RuntimeException(
        s"Command failed with exit code $exitCode:${cmd.mkString(" ")}"
      )
  }

  def runShell(
    image: String,
    shellCommand: Seq[String],
    extraArgs: Seq[String] = Seq.empty
  ): Unit = {
    run(
      image = image,
      extraArgs = extraArgs ++ Seq("--entrypoint", "bash"),
      args = Seq("-c", shellCommand.mkString(" "))
    )
  }

  def makeRunCmd(
    image: String,
    args: Seq[String] = Seq.empty,
    extraArgs: Seq[String] = Seq.empty
  ): Seq[String] = {
    Seq(
      "docker",
      "run",
      "--rm",
      "-t"
    ) ++ extraArgs ++ Seq(
      image
    ) ++ args
  }

  def prepare(cmd: Seq[String]): ProcessBuilder = {
    logger.debug("Docker run: " + cmd.mkString(" "))
    Process(cmd)
  }

  def kill(container: String, signal: String = "TERM"): Unit = {
    val processBuilder = prepare(
      Seq("docker", "kill", s"--signal=$signal", container)
    )
    processBuilder.!
  }

  def inspectHostPort(container: String, port: String): String = {
    val format = "--format={{ (index (index .NetworkSettings.Ports \"" + port + "\") 0).HostPort }}"
    val processBuilder = prepare(Seq("docker", "inspect", format, container))
    processBuilder.!!.stripLineEnd
  }
}
