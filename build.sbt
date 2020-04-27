import sbtassembly.AssemblyPlugin.defaultUniversalScript

name := "kamu-cli"
organization in ThisBuild := "dev.kamu"
organizationName in ThisBuild := "kamu.dev"
startYear in ThisBuild := Some(2018)
licenses in ThisBuild += ("MPL-2.0", new URL(
  "https://www.mozilla.org/en-US/MPL/2.0/"
))
scalaVersion in ThisBuild := "2.11.12"

//////////////////////////////////////////////////////////////////////////////
// Projects
//////////////////////////////////////////////////////////////////////////////

lazy val kamuCli = project
  .in(file("."))
  .dependsOn(
    kamuCoreUtils % "compile->compile;test->test",
    kamuCoreManifests,
    kamuCoreIngestPolling,
    kamuCoreTransformStreaming
  )
  .aggregate(
    kamuCoreUtils,
    kamuCoreManifests,
    kamuCoreIngestPolling,
    kamuCoreTransformStreaming
  )
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    aggregate in assembly := false,
    libraryDependencies ++= Seq(
      deps.jcabiLog,
      deps.scallop,
      deps.hadoopCommon,
      deps.sqlLine,
      deps.scalajHttp,
      deps.json4sJackson,
      deps.jacksonCore,
      deps.jacksonDatabind
    ),
    commonSettings,
    sparkTestingSettings,
    assemblySettings
  )

lazy val kamuCoreUtils = project
  .in(file("core.utils"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    libraryDependencies ++= Seq(
      deps.hadoopCommon,
      deps.scalaTest % "test",
      deps.sparkCore % "provided",
      deps.sparkHive % "provided",
      deps.geoSpark % "test",
      deps.geoSparkSql % "test",
      deps.sparkTestingBase % "test",
      deps.sparkHive % "test"
    ),
    commonSettings,
    sparkTestingSettings
  )

lazy val kamuCoreManifests = project
  .in(file("core.manifests"))
  .dependsOn(
    kamuCoreUtils % "compile->compile;test->test"
  )
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    libraryDependencies ++= Seq(
      deps.hadoopCommon,
      deps.pureConfig,
      deps.pureConfigYaml,
      deps.spire
    ),
    commonSettings
  )

lazy val kamuCoreIngestPolling = project
  .in(file("core.ingest.polling"))
  .enablePlugins(AutomateHeaderPlugin)
  .dependsOn(
    kamuCoreUtils % "compile->compile;test->test",
    kamuCoreManifests
  )
  .settings(
    libraryDependencies ++= Seq(
      deps.sparkCore % "provided",
      deps.sparkSql % "provided",
      deps.geoSpark % "provided",
      deps.geoSparkSql % "provided"
    ),
    commonSettings,
    sparkTestingSettings
  )

lazy val kamuCoreTransformStreaming = project
  .in(file("core.transform.streaming"))
  .enablePlugins(AutomateHeaderPlugin)
  .dependsOn(
    kamuCoreUtils % "compile->compile;test->test",
    kamuCoreManifests
  )
  .settings(
    libraryDependencies ++= Seq(
      deps.sparkCore % "provided",
      deps.sparkSql % "provided",
      deps.geoSpark % "provided",
      deps.geoSparkSql % "provided"
    ),
    commonSettings,
    sparkTestingSettings
  )

//////////////////////////////////////////////////////////////////////////////
// Depencencies
//////////////////////////////////////////////////////////////////////////////

lazy val versions = new {
  val akka = "2.5.22"
  val akkaHttp = "10.1.8"
  val geoSpark = "1.2.0"
  val hadoopCommon = "2.6.5"
  val json4sJackson = "3.5.3"
  val jacksonCore = "2.6.7"
  val jacksonDatabind = "2.6.7.1"
  val pureConfig = "0.11.1"
  val scalajHttp = "2.4.1"
  val spark = "2.4.0"
  val sparkTestingBase = s"${spark}_0.11.0"
  val spire = "0.13.0" // Used by spark too
}

lazy val deps =
  new {
    val jcabiLog = "com.jcabi" % "jcabi-log" % "0.17.3"
    val scallop = "org.rogach" %% "scallop" % "3.3.1"
    // Configs
    val pureConfig = "com.github.pureconfig" %% "pureconfig" % versions.pureConfig
    val pureConfigYaml = "com.github.pureconfig" %% "pureconfig-yaml" % versions.pureConfig
    // Ingest
    val scalajHttp = "org.scalaj" %% "scalaj-http" % versions.scalajHttp
    val json4sJackson =
      ("org.json4s" %% "json4s-jackson" % versions.json4sJackson)
        .exclude("com.fasterxml.jackson.core", "jackson-core")
        .exclude("com.fasterxml.jackson.core", "jackson-databind")
    val jacksonCore = "com.fasterxml.jackson.core" % "jackson-core" % versions.jacksonCore
    val jacksonDatabind = "com.fasterxml.jackson.core" % "jackson-databind" % versions.jacksonDatabind
    // Spark
    val sparkCore = "org.apache.spark" %% "spark-core" % versions.spark
    val sparkSql = "org.apache.spark" %% "spark-sql" % versions.spark
    // GeoSpark
    val geoSpark = "org.datasyslab" % "geospark" % versions.geoSpark
    val geoSparkSql = "org.datasyslab" % "geospark-sql_2.3" % versions.geoSpark
    // Hadoop File System
    val hadoopCommon =
      ("org.apache.hadoop" % "hadoop-common" % versions.hadoopCommon)
        .exclude("commons-beanutils", "commons-beanutils")
        .exclude("commons-beanutils", "commons-beanutils-core")
    // SQL Shell
    val sqlLine = "sqlline" % "sqlline" % "1.8.0"
    // Math
    // TODO: Using older version as it's also used by Spark
    //val spire = "org.typelevel" %% "spire" % versions.spire
    val spire = "org.spire-math" %% "spire" % versions.spire
    // Test
    val scalaTest = "org.scalatest" %% "scalatest" % "3.0.8"
    val sparkHive = "org.apache.spark" %% "spark-hive" % versions.spark
    val sparkTestingBase = "com.holdenkarau" %% "spark-testing-base" % versions.sparkTestingBase
  }

//////////////////////////////////////////////////////////////////////////////
// Settings
//////////////////////////////////////////////////////////////////////////////

lazy val commonSettings = Seq()

lazy val sparkTestingSettings = Seq(
  fork in Test := true,
  parallelExecution in Test := false,
  javaOptions ++= Seq(
    "-Xms512M",
    "-Xmx2048M",
    "-XX:+CMSClassUnloadingEnabled"
  )
)

lazy val assemblySettings = Seq(
  mainClass in assembly := Some("dev.kamu.cli.KamuApp"),
  assemblyJarName in assembly := "kamu",
  assemblyOption in assembly := (assemblyOption in assembly).value
    .copy(prependShellScript = Some(defaultUniversalScript(shebang = true))),
  assemblyMergeStrategy in assembly := {
    // TODO: begin hive fat jar insanity
    case PathList("META-INF", "native", xs @ _*)           => MergeStrategy.last
    case PathList("com", "google", "common", xs @ _*)      => MergeStrategy.last
    case PathList("javax", xs @ _*)                        => MergeStrategy.last
    case PathList("jline", xs @ _*)                        => MergeStrategy.last
    case PathList("org", "apache", "commons", xs @ _*)     => MergeStrategy.last
    case PathList("org", "apache", "http", xs @ _*)        => MergeStrategy.last
    case PathList("org", "codehaus", xs @ _*)              => MergeStrategy.last
    case PathList("org", "fusesource", "hawtjni", xs @ _*) => MergeStrategy.last
    case PathList("org", "fusesource", "jansi", xs @ _*)   => MergeStrategy.last
    case PathList("org", "slf4j", xs @ _*)                 => MergeStrategy.last
    case PathList("org", "xerial", xs @ _*)                => MergeStrategy.last
    case PathList("com", "thoughtworks", "paranamer", xs @ _*) =>
      MergeStrategy.last
    // end insanity
    case "overview.html" => MergeStrategy.discard
    case "plugin.xml"    => MergeStrategy.discard
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  },
  test in assembly := {}
)
