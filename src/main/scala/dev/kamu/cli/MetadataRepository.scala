/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli

import java.io.PrintWriter

import dev.kamu.core.manifests.{
  Dataset,
  DatasetID,
  Volume,
  VolumeID,
  VolumeLayout
}
import dev.kamu.cli.utility.DependencyGraph
import org.apache.hadoop.fs.{FileSystem, Path}
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import java.net.URI

import yaml.defaults._
import pureconfig.generic.auto._
import dev.kamu.core.utils.fs._
import org.apache.log4j.LogManager

class MetadataRepository(
  fileSystem: FileSystem,
  workspaceLayout: WorkspaceLayout
) {
  private val logger = LogManager.getLogger(getClass.getName)

  protected val datasetRepo =
    new GenericResourceRepository[Dataset, DatasetID](
      fileSystem,
      workspaceLayout.datasetsDir,
      "dataset",
      DatasetID,
      (ds: Dataset) => ds.id
    )

  protected val volumeRepo =
    new GenericResourceRepository[Volume, VolumeID](
      fileSystem,
      workspaceLayout.volumesDir,
      "volume",
      VolumeID,
      (vol: Volume) => vol.id
    )

  ////////////////////////////////////////////////////////////////////////////
  // Datasets
  ////////////////////////////////////////////////////////////////////////////

  def getDataset(id: DatasetID): Dataset = {
    datasetRepo.getResource(id)
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
    datasetRepo.getAllResourceIDs()
  }

  def getAllDatasets(): Seq[Dataset] = {
    datasetRepo.getAllResources()
  }

  def loadDatasetFromURI(uri: URI): Dataset = {
    datasetRepo.loadResourceFromURI(uri)
  }

  def addDataset(ds: Dataset): Unit = {
    if (datasetRepo.getResourceOpt(ds.id).isDefined)
      throw new AlreadyExistsException(ds.id.toString, "dataset")

    try {
      ds.dependsOn.map(getDataset)
      ds.remoteSource.map(rs => getVolume(rs.volumeID))
    } catch {
      case e: DoesNotExistException =>
        throw new MissingReferenceException(
          ds.id.toString,
          "dataset",
          e.id,
          e.kind
        )
    }

    datasetRepo.addResource(ds)
  }

  def deleteDataset(id: DatasetID): Unit = {
    // Validate references
    val referencedBy = getAllDatasets().filter(_.dependsOn.contains(id))
    if (referencedBy.nonEmpty)
      throw new DanglingReferenceException(referencedBy.map(_.id), id)

    getAllDataPaths(id).foreach(p => fileSystem.delete(p, true))

    datasetRepo.deleteResource(id)
  }

  def purgeDataset(id: DatasetID): Unit = {
    getDataset(id)
    getAllDataPaths(id).foreach(p => fileSystem.delete(p, true))
    // TODO: Purging a dataset that is used by non-empty derivatives should raise an error
  }

  def exportDataset(ds: Dataset, path: Path): Unit = {
    datasetRepo.saveResourceToFile(ds, path)
  }

  protected def getAllDataPaths(id: DatasetID): Seq[Path] = {
    val volumeLayout = getLocalVolume()

    Seq(
      volumeLayout.dataDir.resolve(id.toString),
      volumeLayout.cacheDir.resolve(id.toString),
      volumeLayout.checkpointsDir.resolve(id.toString)
    )
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
      dataDir = workspaceLayout.localVolumeDir.resolve("data"),
      cacheDir = workspaceLayout.localVolumeDir.resolve("cache")
    )
  }

  def loadVolumeFromURI(uri: URI): Volume = {
    volumeRepo.loadResourceFromURI(uri)
  }

  def getAllVolumeIDs(): Seq[VolumeID] = {
    volumeRepo.getAllResourceIDs()
  }

  def getAllVolumes(): Seq[Volume] = {
    volumeRepo.getAllResources()
  }

  def getVolume(volumeID: VolumeID): Volume = {
    volumeRepo.getResource(volumeID)
  }

  def addVolume(volume: Volume): Unit = {
    volumeRepo.addResource(volume)
  }

  def deleteVolume(volumeID: VolumeID): Unit = {
    // TODO: validate references
    volumeRepo.deleteResource(volumeID)
  }

}
