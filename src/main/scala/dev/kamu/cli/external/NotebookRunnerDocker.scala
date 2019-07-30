package dev.kamu.cli.external

import dev.kamu.cli.RepositoryVolumeMap
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager
import sun.misc.{Signal, SignalHandler}

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
      jupyter.chown()
    }
  }

  def createNetwork(network: String): Unit = {
    new DockerClient().prepare(Seq("docker", "network", "create", network)).!
  }
}
