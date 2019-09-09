object Versions {
  val spark = "2.4.0"
  val geoSpark = "1.2.0"
  val hiveJDBC = sys.props
    .get("hive.jdbc.version")
    .map {
      case "upstream" => "1.2.1.spark2"
      case other      => other
    }
    .getOrElse("1.2.1.spark2.kamu.1")
}
