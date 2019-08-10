package dev.kamu.cli.external

import java.io.{InputStream, OutputStream}
import java.nio.charset.StandardCharsets

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

  def run(processIO: Option[ProcessIO] = None): DockerProcess = {
    val processBuilder = dockerClient.prepare(cmd)
    new DockerProcess(
      id,
      dockerClient,
      runArgs.containerName.get,
      processBuilder,
      runArgs,
      processIO
    )
  }
}

class DockerProcess(
  id: String,
  dockerClient: DockerClient,
  containerName: String,
  processBuilder: ProcessBuilder,
  runArgs: DockerRunArgs,
  ioHandler: Option[ProcessIO] = None
) {
  val process: Process = processBuilder.run(getIOHandler())

  protected def getIOHandler(): ProcessIO = {
    if (ioHandler.isDefined)
      ioHandler.get
    else if (runArgs.interactive)
      IOHandlerPresets.interactive()
    else
      IOHandlerPresets.redirectOutputTagged(s"[$id] ")
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

object IOHandlerPresets {
  def interactive(): ProcessIO = {
    new ProcessIO(
      in =>
        while (true) {
          val line = scala.io.StdIn.readLine()

          if (line == null)
            in.close()
          else {
            in.write((line + "\n").getBytes(StandardCharsets.UTF_8))
            in.flush()
          }
        },
      out => stream(out, System.out),
      err => stream(err, System.err)
    )
  }

  def redirectOutputTagged(tag: String): ProcessIO = {
    new ProcessIO(
      _ => (),
      out =>
        scala.io.Source
          .fromInputStream(out)
          .getLines
          .foreach(l => System.out.println(tag + l)),
      stderr =>
        scala.io.Source
          .fromInputStream(stderr)
          .getLines()
          .foreach(l => System.err.println(tag + l))
    )
  }

  def blackHoled(): ProcessIO = {
    new ProcessIO(
      _ => (),
      _ => (),
      _ => ()
    )
  }

  private def stream(from: InputStream, to: OutputStream): Unit = {
    val buf = new Array[Byte](1024)
    while (true) {
      val read = from.read(buf)
      if (read < 0)
        return

      to.write(buf, 0, read)
    }
  }
}
