package dev.kamu.cli.output

class SimpleResultSet {
  var columns = Vector.empty[String]
  var rows = Vector.empty[Array[Any]]

  def addColumn(name: String): Unit = {
    columns = columns :+ name
  }

  def addColumns(names: String*): Unit = {
    columns = columns ++ names
  }

  def addRow(values: Any*): Unit = {
    rows = rows :+ values.toArray
  }
}
