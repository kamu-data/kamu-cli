import sbtassembly.AssemblyPlugin.defaultUniversalScript

lazy val kamuCoreManifests = RootProject(file("../kamu-core-manifests"))

lazy val kamuCoreIngestPolling = RootProject(
  file("../kamu-core-ingest-polling")
)

lazy val kamuCoreTransformStreaming = RootProject(
  file("../kamu-core-transform-streaming")
)

lazy val kamuCli = (project in file("."))
  .aggregate(
    kamuCoreManifests,
    kamuCoreIngestPolling,
    kamuCoreTransformStreaming
  )
  .dependsOn(
    kamuCoreManifests,
    kamuCoreIngestPolling,
    kamuCoreTransformStreaming
  )
  .settings(
    scalaVersion := "2.11.12",
    organization := "dev.kamu",
    organizationName := "kamu",
    name := "kamu-cli",
    version := "0.1.0-SNAPSHOT",
    libraryDependencies ++= Seq(
      "dev.kamu" %% "kamu-core-manifests" % "0.1.0-SNAPSHOT",
      "dev.kamu" %% "kamu-core-ingest-polling" % "0.1.0-SNAPSHOT",
      "dev.kamu" %% "kamu-core-transform-streaming" % "0.1.0-SNAPSHOT",
      "com.jcabi" % "jcabi-log" % "0.17.3",
      "com.github.scopt" %% "scopt" % "4.0.0-RC2",
      ("org.apache.hadoop" % "hadoop-common" % "2.6.5")
        .exclude("commons-beanutils", "commons-beanutils")
        .exclude("commons-beanutils", "commons-beanutils-core"),
      "org.scalatest" %% "scalatest" % "3.0.5" % Test
    ),
    mainClass in assembly := Some("dev.kamu.cli.KamuApp"),
    assemblyJarName in assembly := "kamu",
    assemblyOption in assembly := (assemblyOption in assembly).value
      .copy(prependShellScript = Some(defaultUniversalScript(shebang = true))),
    assemblyMergeStrategy in assembly := {
      case "overview.html" => MergeStrategy.discard
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
    test in assembly := {}
  )
