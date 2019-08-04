package dev.kamu.cli.external

import org.apache.hadoop.fs.Path
import org.apache.log4j.LogManager

import scala.sys.process.{Process, ProcessBuilder}

case class DockerRunArgs(
  image: String,
  args: List[String] = List.empty,
  containerName: Option[String] = None,
  hostname: Option[String] = None,
  network: Option[String] = None,
  exposeAllPorts: Boolean = false,
  exposePorts: List[Int] = List.empty,
  exposePortMap: Map[Int, Int] = Map.empty,
  volumeMap: Map[Path, Path] = Map.empty,
  environmentVars: Map[String, String] = Map.empty,
  entryPoint: Option[String] = None,
  remove: Boolean = true,
  tty: Boolean = false,
  interactive: Boolean = false
)

class DockerClient {
  protected val logger = LogManager.getLogger(getClass.getName)

  def run(
    runArgs: DockerRunArgs
  ): Unit = {
    val cmd = makeRunCmd(runArgs)
    val process = prepare(cmd)
    val exitCode = process.!
    if (exitCode != 0)
      throw new RuntimeException(
        s"Command failed with exit code $exitCode:${cmd.mkString(" ")}"
      )
  }

  def runShell(
    runArgs: DockerRunArgs,
    shellCommand: Seq[String]
  ): Unit = {
    run(
      runArgs.copy(
        entryPoint = Some("bash"),
        args = List("-c", shellCommand.mkString(" "))
      )
    )
  }

  def makeRunCmd(runArgs: DockerRunArgs): Seq[String] = {
    List(
      List(
        "docker",
        "run"
      ),
      if (runArgs.remove) List("--rm") else List.empty,
      if (runArgs.tty) List("-t") else List.empty,
      if (runArgs.interactive) List("-i") else List.empty,
      runArgs.containerName.map(v => List("--name", v)).getOrElse(List.empty),
      runArgs.hostname.map(v => List("--hostname", v)).getOrElse(List.empty),
      runArgs.network.map(v => List("--network", v)).getOrElse(List.empty),
      if (runArgs.exposeAllPorts) List("-P") else List.empty,
      runArgs.exposePorts
        .map(p => List("-p", p.toString))
        .reduceOption(_ ++ _)
        .getOrElse(List.empty),
      runArgs.exposePortMap
        .map { case (h, c) => List("-p", s"$h:$c") }
        .reduceOption(_ ++ _)
        .getOrElse(List.empty),
      runArgs.volumeMap
        .map {
          case (h, c) => List("-v", s"${h.toUri.getPath}:${c.toUri.getPath}")
        }
        .reduceOption(_ ++ _)
        .getOrElse(List.empty),
      runArgs.environmentVars
        .map { case (n, v) => List("-e", s"$n=$v") }
        .reduceOption(_ ++ _)
        .getOrElse(List.empty),
      runArgs.entryPoint
        .map(v => List("--entrypoint", v))
        .getOrElse(List.empty),
      List(runArgs.image),
      runArgs.args
    ).reduce(_ ++ _)
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
