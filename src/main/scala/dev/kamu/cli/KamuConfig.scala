package dev.kamu.cli

case class KamuConfig(
  spark: SparkConfig = SparkConfig()
)

case class SparkConfig(
  driverMemory: String = "2g"
)
