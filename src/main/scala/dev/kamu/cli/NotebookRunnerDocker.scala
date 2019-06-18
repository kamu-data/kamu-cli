package dev.kamu.cli

import java.awt.Desktop
import java.net.URI

import dev.kamu.core.manifests.RepositoryVolumeMap
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager
import sun.misc.{Signal, SignalHandler}

import scala.sys.process.{Process, ProcessIO}

class NotebookRunnerDocker(
  fileSystem: FileSystem,
  repositoryVolumeMap: RepositoryVolumeMap
) {
  protected val logger = LogManager.getLogger(getClass.getName)

  def start(): Unit = {
    val network = "kamu"
    createNetwork(network)

    val livy = new LivyProcess(fileSystem, repositoryVolumeMap, network)
    val jupyter = new JupyterProcess(fileSystem, network)

    Signal.handle(new Signal("INT"), new SignalHandler {
      override def handle(signal: Signal): Unit = {
        jupyter.stop()
        livy.stop()
      }
    })

    try {
      val livyProcess = livy.run()
      val jupyterProcess = jupyter.run()
      jupyter.openBrowserWhenReady()
      jupyterProcess.exitValue()
      livyProcess.exitValue()
    } finally {
      jupyter.stop()
      livy.stop()
    }
  }

  def createNetwork(network: String): Unit = {
    val cmd = Seq("docker", "network", "create", network)
    logger.debug("Docker cmd: " + cmd.mkString(" "))
    Process(cmd).!
  }
}

class LivyProcess(
  fileSystem: FileSystem,
  repositoryVolumeMap: RepositoryVolumeMap,
  network: String,
  image: String = "kamu/spark-py:2.4.0_0.0.1"
) {
  protected val logger = LogManager.getLogger(getClass.getName)

  val containerName = "kamu-livy"

  def run(): Process = {
    val cmd = Seq(
      "docker",
      "run",
      "--rm",
      "-t",
      "-p",
      "8998",
      "--hostname",
      containerName,
      "--network",
      network,
      "--name",
      containerName,
      image,
      "livy"
    )

    logger.debug("Docker cmd: " + cmd.mkString(" "))

    val processBuilder = Process(cmd)
    processBuilder.run(ioHandler)
  }

  def stop(): Unit = {
    val killCmd = Seq("docker", "kill", "--signal=TERM", containerName)
    logger.debug("Docker cmd: " + killCmd.mkString(" "))
    Process(killCmd).!
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

class JupyterProcess(
  fileSystem: FileSystem,
  network: String,
  image: String = "kamu/jupyter:0.0.1"
) {
  protected val logger = LogManager.getLogger(getClass.getName)

  val containerName = "kamu-jupyter"
  var port: Long = 0
  var token: String = ""

  def run(): Process = {
    val cmd = Seq(
      "docker",
      "run",
      "--rm",
      "-t",
      "--network",
      network,
      "-v",
      s"${fileSystem.getWorkingDirectory.toUri.getPath}:/opt/workdir",
      "-P",
      "--name",
      containerName,
      image
    )

    logger.debug("Docker cmd: " + cmd.mkString(" "))

    val processBuilder = Process(cmd)
    processBuilder.run(ioHandler)
  }

  def ioHandler: ProcessIO = {
    val tokenRegex = raw"token=([a-z0-9]+)".r

    new ProcessIO(
      _ => (),
      stdout => {
        val lines = scala.io.Source
          .fromInputStream(stdout)
          .getLines

        for (line <- lines) {
          synchronized {
            if (token.isEmpty) {
              token = tokenRegex
                .findFirstMatchIn(line)
                .map(m => m.group(1))
                .getOrElse("")
              if (token.nonEmpty) {
                logger.debug(s"Got Jupyter token: $token")
                this.notifyAll()
              }
            }
          }

          println("[jupyter] " + line)
        }

      },
      stderr =>
        scala.io.Source
          .fromInputStream(stderr)
          .getLines()
          .foreach(l => System.err.println("[jupyter] " + l))
    )
  }

  def openBrowserWhenReady(): Unit = {
    if (Desktop.isDesktopSupported && Desktop.getDesktop.isSupported(
          Desktop.Action.BROWSE
        )) {
      val pi = this

      val browserOpenerThread = new Thread {
        override def run(): Unit = {
          pi.synchronized {
            while (pi.token.isEmpty) {
              pi.wait()
            }
          }
          val containerPort = dockerGetHostPort(containerName, "80/tcp")
          val uri = URI.create(s"http://localhost:$containerPort/?token=$token")

          logger.info(s"Opening in browser: $uri")
          Desktop.getDesktop.browse(uri)
        }
      }

      browserOpenerThread.setDaemon(true)
      browserOpenerThread.start()
    }
  }

  def dockerGetHostPort(container: String, port: String): String = {
    val format = "--format={{ (index (index .NetworkSettings.Ports \"" + port + "\") 0).HostPort }}"
    val cmd = Seq("docker", "inspect", format, container)
    Process(cmd).!!.stripLineEnd
  }

  def stop(): Unit = {
    val killCmd = Seq("docker", "kill", "--signal=TERM", containerName)
    logger.debug("Docker cmd: " + killCmd.mkString(" "))
    Process(killCmd).!
  }
}
