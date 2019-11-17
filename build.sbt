import sbtassembly.AssemblyPlugin.defaultUniversalScript

name := "kamu-cli"
organization in ThisBuild := "dev.kamu"
organizationName in ThisBuild := "kamu"
scalaVersion in ThisBuild := "2.11.12"

//////////////////////////////////////////////////////////////////////////////
// Projects
//////////////////////////////////////////////////////////////////////////////

lazy val kamuCli = project
  .in(file("."))
  .dependsOn(
    kamuCoreManifests,
    kamuCoreIngestPolling,
    kamuCoreTransformStreaming
  )
  .settings(
    libraryDependencies ++= Seq(
      deps.jcabiLog,
      deps.scallop,
      deps.hadoopCommon,
      deps.sqlLine,
      deps.hiveJdbc,
      deps.sparkCore % "provided",
      deps.sparkSql % "provided",
      deps.geoSpark % "provided",
      deps.geoSparkSql % "provided"
    ),
    commonSettings,
    sparkTestingSettings,
    assemblySettings
  )

lazy val kamuCoreManifests = project
  .in(file("core.manifests"))
  .settings(
    libraryDependencies ++= Seq(
      deps.hadoopCommon,
      deps.pureConfig,
      deps.pureConfigYaml,
      deps.scalaTest % "test"
    ),
    commonSettings
  )

lazy val kamuCoreIngestPolling = project
  .in(file("core.ingest.polling"))
  .dependsOn(
    kamuCoreManifests
  )
  .settings(
    libraryDependencies ++= Seq(
      deps.pureConfig,
      deps.pureConfigYaml,
      deps.scalajHttp,
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
  .dependsOn(
    kamuCoreManifests
  )
  .settings(
    libraryDependencies ++= Seq(
      deps.pureConfig,
      deps.pureConfigYaml,
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
  val hiveJdbc = sys.props
    .get("hive.jdbc.version")
    .map {
      case "upstream" => "1.2.1.spark2"
      case other      => other
    }
    .getOrElse("1.2.1.spark2.kamu.1")
  val pureConfig = "0.11.1"
  val scalajHttp = "2.4.1"
  val spark = "2.4.0"
  val sparkTestingBase = s"${spark}_0.11.0"
}

lazy val deps =
  new {
    val jcabiLog = "com.jcabi" % "jcabi-log" % "0.17.3"
    val scallop = "org.rogach" %% "scallop" % "3.3.1"
    // Configs
    val pureConfig = "com.github.pureconfig" %% "pureconfig" % versions.pureConfig
    val pureConfigYaml = "com.github.pureconfig" %% "pureconfig-yaml" % versions.pureConfig
    // HTTP
    val scalajHttp = "org.scalaj" %% "scalaj-http" % versions.scalajHttp
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
    // NOTE: Using kamu-specific Hive version with some bugfixes
    // remove `kamu.X` part if you want a simpler build
    val hiveJdbc = ("org.spark-project.hive" % "hive-jdbc" % versions.hiveJdbc)
      .excludeAll(ExclusionRule(organization = "log4j"))
      .excludeAll(ExclusionRule(organization = "org.apache.geronimo.specs"))
      .exclude("org.apache.hadoop", "hadoop-yarn-api")
      .exclude("org.fusesource.leveldbjni", "leveldbjni-all")
    // Test
    val scalaTest = "org.scalatest" %% "scalatest" % "3.0.8"
    val sparkHive = "org.apache.spark" %% "spark-hive" % versions.spark
    val sparkTestingBase = "com.holdenkarau" %% "spark-testing-base" % versions.sparkTestingBase
  }

//////////////////////////////////////////////////////////////////////////////
// Settings
//////////////////////////////////////////////////////////////////////////////

lazy val commonSettings = Seq(
  resolvers += Resolver.mavenLocal
)

lazy val sparkTestingSettings = Seq(
  libraryDependencies ++= Seq(
    deps.scalaTest % "test",
    deps.sparkTestingBase % "test",
    deps.sparkHive % "test"
  ),
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
    case "overview.html" => MergeStrategy.discard
    case "plugin.xml"    => MergeStrategy.discard
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  },
  test in assembly := {}
)
