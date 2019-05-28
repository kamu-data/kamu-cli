import sbtassembly.AssemblyPlugin.defaultUniversalScript

lazy val kamuCoreManifests = RootProject(file("../kamu-core-manifests"))

lazy val kamuCoreIngestPolling = RootProject(
  file("../kamu-core-ingest-polling"))

lazy val kamuCliMacros = (project in file("./macros"))

lazy val kamuCli = (project in file("."))
  .aggregate(kamuCliMacros, kamuCoreManifests, kamuCoreIngestPolling)
  .dependsOn(kamuCliMacros, kamuCoreManifests, kamuCoreIngestPolling)
  .settings(
    scalaVersion := "2.11.12",
    organization := "dev.kamu",
    organizationName := "kamu",
    name := "kamu-cli",
    version := "0.1.0-SNAPSHOT",
    libraryDependencies ++= Seq(
      "dev.kamu" %% "kamu-core-manifests" % "0.1.0-SNAPSHOT",
      "dev.kamu" %% "kamu-core-ingest-polling" % "0.1.0-SNAPSHOT",
      "com.github.scopt" %% "scopt" % "4.0.0-RC2",
      "org.scalatest" %% "scalatest" % "3.0.5" % Test
    ),
    mainClass in assembly := Some("dev.kamu.cli.KamuApp"),
    assemblyJarName in assembly := "kamu",
    assemblyOption in assembly := (assemblyOption in assembly).value
      .copy(prependShellScript = Some(defaultUniversalScript(shebang = true))),
    test in assembly := {}
  )
