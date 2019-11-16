package dev.kamu.cli

import java.io.PrintWriter

import dev.kamu.core.manifests.{Dataset, DatasetID, Manifest, VolumeLayout}
import dev.kamu.cli.utility.DependencyGraph
import org.apache.hadoop.fs.{FileSystem, Path}
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import java.net.URI

import yaml.defaults._
import pureconfig.generic.auto._
import dev.kamu.core.manifests.utils.fs._
import org.apache.log4j.LogManager

class MetadataRepository(
  fileSystem: FileSystem,
  workspaceLayout: WorkspaceLayout
) {
  private val logger = LogManager.getLogger(getClass.getName)

  ////////////////////////////////////////////////////////////////////////////
  // Datasets
  ////////////////////////////////////////////////////////////////////////////

  def getDataset(id: DatasetID): Dataset = {
    val path = getDatasetDefinitionPath(id)

    if (!fileSystem.exists(path))
      throw new DoesNotExistsException(id)

    loadDatasetFromFile(path)
  }

  def getDatasetsInDependencyOrder(
    ids: Seq[DatasetID],
    recursive: Boolean
  ): Seq[Dataset] = {
    if (recursive) {
      val dependsOn = getDataset(_: DatasetID).dependsOn.toList
      val depGraph = new DependencyGraph[DatasetID](dependsOn)
      depGraph.resolve(ids.toList).map(getDataset)
    } else {
      val dependsOn = getDataset(_: DatasetID).dependsOn.toList
        .filter(ids.contains)
      val depGraph = new DependencyGraph[DatasetID](dependsOn)
      depGraph.resolve(ids.toList).map(getDataset)
    }
  }

  def getAllDatasetIDs(): Seq[DatasetID] = {
    fileSystem
      .listStatus(workspaceLayout.datasetsDir)
      .map(_.getPath.getName)
      .map(filename => filename.substring(0, filename.length - ".yaml".length))
      .map(DatasetID)
  }

  def getAllDatasets(): Seq[Dataset] = {
    val manifestFiles = fileSystem
      .listStatus(workspaceLayout.datasetsDir)
      .map(_.getPath)

    manifestFiles.map(loadDatasetFromFile)
  }

  def loadDatasetFromFile(p: Path): Dataset = {
    val inputStream = fileSystem.open(p)
    try {
      yaml.load[Manifest[Dataset]](inputStream).content
    } catch {
      case e: Exception =>
        logger.error(s"Error while loading data source manifest from file: $p")
        throw e
    } finally {
      inputStream.close()
    }
  }

  def loadDatasetFromURI(uri: URI): Dataset = {
    uri.getScheme match {
      case "https" => loadDatasetFromURL(uri.toURL)
      case "http"  => loadDatasetFromURL(uri.toURL)
      case "file"  => loadDatasetFromFile(new Path(uri.getPath))
      case s       => throw new SchemaNotSupportedException(s)
    }
  }

  def loadDatasetFromURL(url: java.net.URL): Dataset = {
    val source = scala.io.Source.fromURL(url)
    try {
      yaml.load[Manifest[Dataset]](source.mkString).content
    } catch {
      case e: Exception =>
        logger.error(s"Error while loading data source manifest from URL: $url")
        throw e
    } finally {
      source.close()
    }
  }

  def addDataset(ds: Dataset): Unit = {
    val path = getDatasetDefinitionPath(ds.id)

    if (fileSystem.exists(path))
      throw new AlreadyExistsException(ds.id)

    try {
      ds.dependsOn.map(getDataset)
    } catch {
      case e: DoesNotExistsException =>
        throw new MissingReferenceException(ds.id, e.datasetID)
    }

    saveDataset(ds, path)
  }

  def saveDataset(ds: Dataset, path: Path): Unit = {
    val outputStream = fileSystem.create(path)

    try {
      yaml.save(ds.asManifest, outputStream)
    } catch {
      case e: Exception =>
        outputStream.close()
        fileSystem.delete(path, false)
        throw e
    } finally {
      outputStream.close()
    }
  }

  def deleteDataset(id: DatasetID): Unit = {
    // Validate references
    val referencedBy = getAllDatasets().filter(_.dependsOn.contains(id))
    if (referencedBy.nonEmpty)
      throw new DanglingReferenceException(referencedBy.map(_.id), id)

    val path = getDatasetDefinitionPath(id)
    if (!fileSystem.exists(path))
      throw new DoesNotExistsException(id)

    getAllDataPaths(id).foreach(p => fileSystem.delete(p, true))
    fileSystem.delete(path, false)
  }

  def purgeDataset(id: DatasetID): Unit = {
    getDataset(id)
    getAllDataPaths(id).foreach(p => fileSystem.delete(p, true))
    // TODO: Purging a dataset that is used by non-empty derivatives should raise an error
  }

  protected def getAllDataPaths(id: DatasetID): Seq[Path] = {
    val volume = getVolumeFor(id)

    Seq(
      volume.dataDir.resolve(id.toString),
      volume.checkpointsDir.resolve(id.toString),
      getDatasetDefinitionPath(id)
    )
  }

  protected def getDatasetDefinitionPath(id: DatasetID): Path = {
    workspaceLayout.datasetsDir.resolve(id.toString + ".yaml")
  }

  ////////////////////////////////////////////////////////////////////////////
  // Volumes
  ////////////////////////////////////////////////////////////////////////////

  def getLocalVolume(): VolumeLayout = {
    if (!fileSystem.exists(workspaceLayout.localVolumeDir)) {
      fileSystem.mkdirs(workspaceLayout.localVolumeDir)
      val outputStream =
        fileSystem.create(workspaceLayout.localVolumeDir.resolve(".gitignore"))
      val writer = new PrintWriter(outputStream)
      writer.write(WorkspaceLayout.LOCAL_VOLUME_GITIGNORE_CONTENT)
      writer.close()
    }

    VolumeLayout(
      datasetsDir = workspaceLayout.localVolumeDir.resolve("datasets"),
      checkpointsDir = workspaceLayout.localVolumeDir.resolve("checkpoints"),
      dataDir = workspaceLayout.localVolumeDir.resolve("data")
    )
  }

  def getVolumeFor(id: DatasetID): VolumeLayout = {
    // Only local volumes are supported now
    getLocalVolume()
  }
}

/////////////////////////////////////////////////////////////////////////////////////////
// Exceptions
/////////////////////////////////////////////////////////////////////////////////////////

class DoesNotExistsException(val datasetID: DatasetID)
    extends Exception(s"Dataset $datasetID does not exist")

class AlreadyExistsException(val datasetID: DatasetID)
    extends Exception(s"Dataset $datasetID already exists")

class MissingReferenceException(val fromID: DatasetID, val toID: DatasetID)
    extends Exception(s"Dataset $fromID refers to non existent dataset $toID")

class SchemaNotSupportedException(val schema: String)
    extends Exception(s"$schema")

class DanglingReferenceException(
  val fromIDs: Seq[DatasetID],
  val toID: DatasetID
) extends Exception(
      s"Dataset $toID is referenced by: " + fromIDs.mkString(", ")
    )
