package dev.kamu.cli.external

import dev.kamu.core.manifests.VolumeLayout
import org.apache.hadoop.fs.Path

class LivyDockerProcessBuilder(
  volumeLayout: VolumeLayout,
  dockerClient: DockerClient,
  network: Option[String] = None,
  exposeAllPorts: Boolean = false,
  exposePorts: List[Int] = List.empty,
  exposePortMap: Map[Int, Int] = Map.empty
) extends DockerProcessBuilder(
      id = "livy",
      dockerClient = dockerClient,
      runArgs = DockerRunArgs(
        image = DockerImages.LIVY,
        args = List("livy"),
        containerName = Some("kamu-livy"),
        hostname = Some("kamu-livy"),
        exposeAllPorts = exposeAllPorts,
        exposePorts = exposePorts,
        exposePortMap = exposePortMap,
        volumeMap = Map(
          volumeLayout.dataDir -> new Path("/opt/spark/work-dir")
        ),
        network = network
      )
    )
