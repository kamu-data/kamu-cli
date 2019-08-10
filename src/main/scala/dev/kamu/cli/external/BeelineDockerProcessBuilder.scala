package dev.kamu.cli.external

class BeelineDockerProcessBuilder(
  network: Option[String] = None,
  args: Seq[String]
) extends DockerProcessBuilder(
      id = "beeline",
      dockerClient = new DockerClient(),
      runArgs = DockerRunArgs(
        image = "kamu/spark:2.4.0_0.0.1",
        interactive = true,
        entryPoint = Some("/opt/spark/bin/beeline"),
        args = args.toList,
        containerName = Some("kamu-beeline"),
        hostname = Some("kamu-beeline"),
        network = network
      )
    )
