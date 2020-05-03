/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli

import dev.kamu.core.utils.ManualClock
import dev.kamu.core.utils.fs._
import dev.kamu.core.utils.test.KamuDataFrameSuite
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.scalatest._

trait KamuTestBase extends KamuDataFrameSuite { self: Suite =>

  val fileSystem = FileSystem.get(new Configuration())

  def withEmptyDir[T](func: KamuTestAdapter => T): T = {
    Temp.withRandomTempDir(fileSystem, "kamu-test-") { tempDir =>
      val config = KamuConfig(workspaceRoot = tempDir)
      val clock = new ManualClock()
      clock.advance()
      val kamu = new KamuTestAdapter(config, fileSystem, spark, clock)
      func(kamu)
    }
  }

  def withEmptyWorkspace[T](func: KamuTestAdapter => T): T = {
    withEmptyDir { kamu =>
      kamu.run("init")
      func(kamu)
    }
  }

}
