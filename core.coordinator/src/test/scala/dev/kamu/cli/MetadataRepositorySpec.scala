/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli

import java.net.URI
import java.nio.file.Paths

import better.files.File
import pureconfig.generic.auto._
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.cli.metadata.ResourceLoader
import dev.kamu.core.manifests.DatasetID
import dev.kamu.core.utils.fs._
import dev.kamu.core.utils.{DockerClient, DockerProcessBuilder, DockerRunArgs}
import org.scalatest._

import scala.concurrent.duration._

class MetadataRepositorySpec extends FunSuite with Matchers with KamuTestBase {
  protected override val enableHiveSupport = false

  test("getDatasetsInDependencyOrder") {
    withEmptyWorkspace { kamu =>
      // A -> B -> C
      val dsC = DatasetFactory.newRootDataset(
        id = Some(DatasetID("C"))
      )
      val dsB = DatasetFactory.newDerivativeDataset(
        source = dsC.id,
        id = Some(DatasetID("B"))
      )
      val dsA = DatasetFactory.newDerivativeDataset(
        source = dsB.id,
        id = Some(DatasetID("A"))
      )

      kamu.addDataset(dsC)
      kamu.addDataset(dsB)
      kamu.addDataset(dsA)

      val actual1 = kamu.metadataRepository
        .getDatasetsInDependencyOrder(
          Seq(dsA.id, dsB.id, dsC.id),
          recursive = false
        )

      actual1 shouldEqual Seq(dsC.id, dsB.id, dsA.id)

      val actual2 = kamu.metadataRepository
        .getDatasetsInDependencyOrder(
          Seq(dsA.id, dsB.id),
          recursive = false
        )

      actual2 shouldEqual Seq(dsB.id, dsA.id)

      val actual3 = kamu.metadataRepository
        .getDatasetsInDependencyOrder(
          Seq(dsA.id),
          recursive = true
        )

      actual3 shouldEqual Seq(dsC.id, dsB.id, dsA.id)
    }
  }

  test(raw"'kamu add' from HTTP URI") {
    withEmptyWorkspace { kamu =>
      val expected = DatasetFactory.newRootDataset()
      // create a temporary directory with the dataset to host
      val serverDir = kamu.config.workspaceRoot / "server"
      File(serverDir).createDirectories()

      val path = serverDir / "test-dataset.yaml"
      new ResourceLoader().saveResourceToFile(expected, path)

      // start up the server and host the directory
      val serverPort = 80 // httpd:2.4 default port
      val testServerName = "kamu_test_http_server"
      val testHttpServerArgs = DockerRunArgs(
        image = "httpd:2.4",
        exposePorts = List(serverPort),
        volumeMap = Map(serverDir -> Paths.get("/usr/local/apache2/htdocs")), // httpd:2.4 default location
        containerName = Some(testServerName),
        detached = true
      )
      val testHttpServer = new DockerClient()
      try {
        val testHttpServerProc = new DockerProcessBuilder(
          "http",
          testHttpServer,
          testHttpServerArgs
        ).run()
        testHttpServerProc.join()
        val hostPort =
          testHttpServerProc.waitForHostPort(serverPort, 1000 millis) // occasionally fails if set to 500 ms

        // pull the dataset from the server
        val actual =
          kamu.metadataRepository.loadDatasetSnapshotFromURI(
            new URI(s"http://localhost:${hostPort}/test-dataset.yaml")
          )

        actual shouldEqual expected
      } finally {
        // stop the server
        testHttpServer.stop(testServerName)
      }
    }
  }

  test(raw"'kamu add' from file") {
    withEmptyWorkspace { kamu =>
      val expected = DatasetFactory.newRootDataset()
      val testDir = kamu.config.workspaceRoot / "my folder"

      File(testDir).createDirectories()
      val datasetPath = testDir / "test-dataset.yaml"
      new ResourceLoader().saveResourceToFile(expected, datasetPath)

      val actual =
        kamu.metadataRepository.loadDatasetSnapshotFromURI(datasetPath.toUri)

      actual shouldEqual expected
    }
  }
}
