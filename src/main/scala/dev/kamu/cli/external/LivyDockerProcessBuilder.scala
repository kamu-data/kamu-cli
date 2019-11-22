/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

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
