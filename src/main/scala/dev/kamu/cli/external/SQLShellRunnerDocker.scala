package dev.kamu.cli.external

import dev.kamu.cli.RepositoryVolumeMap
import org.apache.log4j.LogManager
import sun.misc.{Signal, SignalHandler}

class SQLShellRunnerDocker(
  repositoryVolumeMap: RepositoryVolumeMap
) {
  protected val logger = LogManager.getLogger(getClass.getName)

  def start(): Unit = {
    val network = "kamu"
    createNetwork(network)

    val livyBuilder =
      new LivyDockerProcessBuilder(repositoryVolumeMap, Some(network))

    val jdbcUrl = s"jdbc:hive2://kamu-livy:10090"

    val beelineBuilder = new BeelineDockerProcessBuilder(
      network = Some("kamu"),
      args = Seq("-u", jdbcUrl)
    )

    var livyProcess: DockerProcess = null
    var beelineProcess: DockerProcess = null

    def stopAll(): Unit = {
      if (livyProcess != null)
        livyProcess.kill()
      if (beelineProcess != null)
        beelineProcess.kill()
    }

    Signal.handle(new Signal("INT"), new SignalHandler {
      override def handle(signal: Signal): Unit = {
        stopAll()
      }
    })

    try {
      livyProcess = livyBuilder.run()

      // TODO: avoid sleeping
      Thread.sleep(3000)

      beelineProcess = beelineBuilder.run(Some(IOHandlerPresets.interactive()))
      beelineProcess.join()
    } finally {
      stopAll()
    }
  }

  def createNetwork(network: String): Unit = {
    new DockerClient().prepare(Seq("docker", "network", "create", network)).!
  }
}
