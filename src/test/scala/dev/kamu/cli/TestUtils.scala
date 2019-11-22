/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, when}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.scalatest.Suite

object TestUtils {

  def ignoreNullableSchema(df: DataFrame): DataFrame = {
    // Coalesce makes spark think the column can be nullable
    df.select(
      df.columns.map(
        c =>
          when(df(c).isNotNull, df(c))
            .otherwise(lit(null))
            .as(c)
      ): _*
    )
  }
}

trait DataFrameSuiteBaseEx extends DataFrameSuiteBase { self: Suite =>

  override def conf: SparkConf = {
    super.conf
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    GeoSparkSQLRegistrator.registerAll(spark)
  }

  def assertSchemasEqual(
    expected: DataFrame,
    actual: DataFrame,
    ignoreNullable: Boolean
  ): Unit = {
    val exp =
      if (ignoreNullable) TestUtils.ignoreNullableSchema(expected) else expected
    val act =
      if (ignoreNullable) TestUtils.ignoreNullableSchema(actual) else actual
    assert(exp.schema, act.schema)
  }

  def assertDataFrameEquals(
    expected: DataFrame,
    actual: DataFrame,
    ignoreNullable: Boolean
  ): Unit = {
    val exp =
      if (ignoreNullable) TestUtils.ignoreNullableSchema(expected) else expected
    val act =
      if (ignoreNullable) TestUtils.ignoreNullableSchema(actual) else actual
    super.assertDataFrameEquals(exp, act)
  }

}
