package dev.kamu.cli

import dev.kamu.core.manifests.{Dataset, DatasetID, RootPollingSource}
import org.scalatest._
import java.net.{URI, URL}

import dev.kamu.cli.external.{
  DockerClient,
  DockerProcess,
  DockerProcessBuilder,
  DockerRunArgs
}
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.concurrent.duration.Duration
import scala.reflect.io.File

class MetadataRepositorySpec extends FunSuite with Matchers with KamuTestBase {
  protected override val enableHiveSupport = false

  test("getDatasetsInDependencyOrder") {
    withEmptyRepo { kamu =>
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
        .map(_.id)

      actual1 shouldEqual Seq(dsC.id, dsB.id, dsA.id)

      val actual2 = kamu.metadataRepository
        .getDatasetsInDependencyOrder(
          Seq(dsA.id, dsB.id),
          recursive = false
        )
        .map(_.id)

      actual2 shouldEqual Seq(dsB.id, dsA.id)

      val actual3 = kamu.metadataRepository
        .getDatasetsInDependencyOrder(
          Seq(dsA.id),
          recursive = true
        )
        .map(_.id)

      actual3 shouldEqual Seq(dsC.id, dsB.id, dsA.id)
    }
  }

  test(raw"'kamu add' from HTTP") {
    withEmptyRepo { kamu =>
      val testDir = getRandomDir()
      val serverPort = 8080
      val hostPort = 9000
      val path: Path = new Path(testDir, raw"test-dataset.yaml")
      fileSystem.mkdirs(testDir)
      val testServerName = s"kamu_test_http_server"
      val testHttpServerArgs = DockerRunArgs(
        image = "python:3.7-slim",
        args = List("python3", "-m", "http.server", s"${serverPort}"),
        exposePortMap = Map(hostPort -> serverPort),
        volumeMap = Map(testDir -> new Path("/mnt/kamu")),
        containerName = Some(testServerName),
        detached = true
      )
      val testHttpServer = new DockerClient(FileSystem.get(new Configuration()))

      val testHttpServerProc = new DockerProcessBuilder(
        "http",
        testHttpServer,
        testHttpServerArgs
      ).run()
      val expected = DatasetFactory.newRootDataset()
      kamu.metadataRepository.saveDataset(expected, path)
      testHttpServerProc.join()
      Thread.sleep(1000) // wait for the port to become ready, 'waitForHostPort' did work :-(
      val actual =
        kamu.metadataRepository.loadDatasetFromURI(
          s"http://localhost:${hostPort}/mnt/kamu/test-dataset.yaml"
        )
      testHttpServer.stop(testServerName, time = 1)
      fileSystem.delete(testDir, true)

      actual shouldEqual expected
    }
  }

  test(raw"'kamu add' from file") {
    withEmptyRepo { kamu =>
      val expected = DatasetFactory.newRootDataset()
      val testDir = getRandomDir()
      val path: Path = new Path(testDir, raw"test-dataset.yaml")
      fileSystem.mkdirs(testDir)
      kamu.metadataRepository.saveDataset(expected, path)
      val actual = kamu.metadataRepository.loadDatasetFromURI(path.toString)
      fileSystem.delete(testDir, true)

      actual shouldEqual expected
    }
  }
}
