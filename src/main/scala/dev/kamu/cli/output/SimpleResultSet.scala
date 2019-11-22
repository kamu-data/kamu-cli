/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

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
