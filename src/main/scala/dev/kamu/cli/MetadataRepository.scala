package dev.kamu.cli

import dev.kamu.core.manifests.{
  DataSource,
  DataSourcePolling,
  Manifest,
  RepositoryVolumeMap,
  TransformStreaming
}
import org.apache.hadoop.fs.{FileSystem, Path}
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import yaml.defaults._
import pureconfig.generic.auto._
import dev.kamu.core.manifests.utils.fs._
import org.apache.log4j.LogManager
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.SafeConstructor

class DoesNotExistsException(s: String) extends Exception(s)
class AlreadyExistsException(s: String) extends Exception(s)

class MetadataRepository(
  fileSystem: FileSystem,
  repositoryVolumeMap: RepositoryVolumeMap
) {
  private val logger = LogManager.getLogger(getClass.getName)

  def getDataSource(id: String): DataSource = {
    val path = repositoryVolumeMap.sourcesDir.resolve(id + ".yaml")

    if (!fileSystem.exists(path))
      throw new DoesNotExistsException(s"Dataset $id does not exist")

    loadDataSource(path)
  }

  def getDataSources(): Seq[DataSource] = {
    val manifestFiles = fileSystem
      .listStatus(repositoryVolumeMap.sourcesDir)
      .map(_.getPath)

    manifestFiles.map(loadDataSource)
  }

  def loadDataSource(p: Path): DataSource = {
    val inputStream = fileSystem.open(p)
    try {

      val rawYaml =
        new Yaml(new SafeConstructor())
          .load[java.util.Map[String, AnyRef]](inputStream)

      if (!rawYaml.containsKey("kind"))
        throw new RuntimeException("Malformed manifest")

      inputStream.seek(0)

      rawYaml.get("kind").asInstanceOf[String] match {
        case "DataSourcePolling" =>
          yaml.load[Manifest[DataSourcePolling]](inputStream).content
        case "TransformStreaming" =>
          yaml.load[Manifest[TransformStreaming]](inputStream).content
      }
    } catch {
      case e: Exception => {
        logger.error(s"Error while loading data source manifest: $p")
        throw e
      }
    } finally {
      inputStream.close()
    }
  }

  def addDataSource(ds: DataSource): Unit = {
    val path =
      repositoryVolumeMap.sourcesDir.resolve(ds.id + ".yaml")

    if (fileSystem.exists(path)) {
      throw new AlreadyExistsException(s"Dataset ${ds.id} already exists")
    } else {
      val outputStream = fileSystem.create(path)

      ds match {
        case ds: DataSourcePolling =>
          yaml.save(ds.asManifest, outputStream)
        case ds: TransformStreaming =>
          yaml.save(ds.asManifest, outputStream)
      }

      outputStream.close()
    }
  }
}
