package dev.kamu.cli

import dev.kamu.core.manifests.VolumeMap
import org.apache.hadoop.fs.{FileSystem, Path}
import dev.kamu.core.manifests.utils.fs._

/** Describes the layout of the data repository on disk */
case class RepositoryVolumeMap(
  /** Directory to store dataset source manifests in */
  sourcesDir: Path,
  /** Directory to store downloaded data in before processing */
  downloadDir: Path,
  /** Directory to store cache information in */
  checkpointDir: Path,
  /** Dataset data directory */
  dataDir: Path
) {

  def allPaths: Seq[Path] = Seq(
    sourcesDir,
    downloadDir,
    checkpointDir,
    dataDir
  )

  def toAbsolute(fs: FileSystem): RepositoryVolumeMap = {
    copy(
      sourcesDir = fs.toAbsolute(sourcesDir),
      downloadDir = fs.toAbsolute(downloadDir),
      checkpointDir = fs.toAbsolute(checkpointDir),
      dataDir = fs.toAbsolute(dataDir)
    )
  }

  def toVolumeMap: VolumeMap = {
    VolumeMap(
      downloadDir = downloadDir,
      checkpointDir = checkpointDir,
      dataDirRoot = dataDir,
      dataDirDeriv = dataDir
    )
  }

}
