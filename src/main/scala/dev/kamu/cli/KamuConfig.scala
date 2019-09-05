package dev.kamu.cli

import org.apache.hadoop.fs.Path
import dev.kamu.core.manifests.utils.fs._

case class KamuConfig(
  repositoryRoot: Path = new Path("."),
  spark: SparkConfig = SparkConfig()
) {
  def kamuRoot: Path = {
    repositoryRoot.resolve(".kamu")
  }
}

case class SparkConfig(
  driverMemory: String = "2g"
)
