package dev.kamu.cli.external

import dev.kamu.cli.RepositoryVolumeMap
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager
import sun.misc.{Signal, SignalHandler}

class NotebookRunnerDocker(
  fileSystem: FileSystem,
  dockerClient: DockerClient,
  repositoryVolumeMap: RepositoryVolumeMap,
  environmentVars: Map[String, String]
) {
  protected val logger = LogManager.getLogger(getClass.getName)

  def start(): Unit = {
    val network = "kamu"
    withNetwork(network) {
      val livyBuilder =
        new LivyDockerProcessBuilder(
          repositoryVolumeMap,
          dockerClient,
          Some(network)
        )

      val jupyterBuilder =
        new JupyterDockerProcessBuilder(
          fileSystem,
          dockerClient,
          network,
          environmentVars
        )

      var livyProcess: DockerProcess = null
      var jupyterProcess: JupyterDockerProcess = null

      def stopAll(): Unit = {
        if (livyProcess != null)
          livyProcess.kill()
        if (jupyterProcess != null)
          jupyterProcess.kill()
      }

      Signal.handle(new Signal("INT"), new SignalHandler {
        override def handle(signal: Signal): Unit = {
          stopAll()
        }
      })

      try {
        livyProcess = livyBuilder.run()
        jupyterProcess = jupyterBuilder.run()
        jupyterProcess.openBrowserWhenReady()
        jupyterProcess.join()
        livyProcess.join()
      } finally {
        stopAll()
        jupyterBuilder.chown()
      }
    }
  }

  def withNetwork[T](network: String)(body: => T): T = {
    dockerClient
      .prepare(Seq("docker", "network", "create", network))
      .!(IOHandlerPresets.logged(logger))

    try {
      body
    } finally {
      dockerClient
        .prepare(Seq("docker", "network", "rm", network))
        .!(IOHandlerPresets.logged(logger))
    }
  }
}
