package dev.kamu.cli.external

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager

import scala.sys.process.{Process, ProcessBuilder, ProcessLogger}
import dev.kamu.core.manifests.utils.fs._

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
  workDir: Option[String] = None,
  environmentVars: Map[String, String] = Map.empty,
  entryPoint: Option[String] = None,
  remove: Boolean = true,
  tty: Boolean = false,
  interactive: Boolean = false,
  detached: Boolean = false
)

class DockerClient(fileSystem: FileSystem) {
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
      if (runArgs.detached) List("-d") else List.empty,
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
          case (h, c) =>
            List(
              "-v",
              s"${fileSystem.toAbsolute(h).toUri.getPath}:${c.toUri.getPath}"
            )
        }
        .reduceOption(_ ++ _)
        .getOrElse(List.empty),
      runArgs.workDir.map(v => List("--workdir", v)).getOrElse(List.empty),
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

  // QA: be aware of '--sig-proxy' and '--stop-signal' options. 'docker kill' default signal is 'kill'
  def kill(container: String, signal: String = "TERM"): Unit = {
    val processBuilder = prepare(
      Seq("docker", "kill", s"--signal=$signal", container)
    )
    processBuilder
      .run(IOHandlerPresets.logged(logger))
      .exitValue()
  }

  // give a container t seconds to terminate, kill otherwise. 10 seconds is docker's default
  def stop(container: String, time: Int = 10): Unit = {
    val processBuilder = prepare(
      Seq("docker", "stop", s"--time=$time", container)
    )
    processBuilder
      .run(IOHandlerPresets.logged(logger))
      .exitValue()
  }

  def pull(image: String): Unit = {
    prepare(Seq("docker", "pull", image)).!
  }

  def inspectHostPort(container: String, port: Int): Option[Int] = {
    val format = "--format={{ (index (index .NetworkSettings.Ports \"" + port + "/tcp\") 0).HostPort }}"
    val processBuilder = prepare(Seq("docker", "inspect", format, container))
    try {
      Some(
        processBuilder
          .!!(IOHandlerPresets.logged(logger))
          .stripLineEnd
          .toInt
      )
    } catch {
      case _: RuntimeException => None
    }
  }
}
