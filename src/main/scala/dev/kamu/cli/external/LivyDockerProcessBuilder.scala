package dev.kamu.cli.external

import dev.kamu.cli.RepositoryVolumeMap
import org.apache.hadoop.fs.Path

class LivyDockerProcessBuilder(
  repositoryVolumeMap: RepositoryVolumeMap,
  network: Option[String] = None,
  exposeAllPorts: Boolean = false,
  exposePorts: List[Int] = List.empty,
  exposePortMap: Map[Int, Int] = Map.empty
) extends DockerProcessBuilder(
      id = "livy",
      dockerClient = new DockerClient(),
      runArgs = DockerRunArgs(
        image = "kamu/spark-py:2.4.0_0.0.1",
        args = List("livy"),
        containerName = Some("kamu-livy"),
        hostname = Some("kamu-livy"),
        exposeAllPorts = exposeAllPorts,
        exposePorts = exposePorts,
        exposePortMap = exposePortMap,
        volumeMap = Map(
          repositoryVolumeMap.dataDir -> new Path("/opt/spark/work-dir")
        ),
        network = network
      )
    )
