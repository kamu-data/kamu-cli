/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli

import java.nio.file.Paths

import dev.kamu.core.utils.AutoClock
import org.apache.logging.log4j.core.config.Configurator
import org.apache.logging.log4j.{Level, LogManager}

class UsageException(message: String = "", cause: Throwable = None.orNull)
    extends RuntimeException(message, cause)

object KamuApp extends App {
  val logger = LogManager.getLogger(getClass.getName)

  val systemClock = new AutoClock()

  val config = KamuConfig(
    workspaceRoot = KamuConfig
      .findWorkspaceRoot(Paths.get(""))
      .getOrElse(Paths.get(""))
  )

  try {
    val cliArgs = new CliArgs(args)

    Configurator.setLevel(
      "dev.kamu",
      if (cliArgs.debug()) Level.ALL else cliArgs.logLevel()
    )

    new Kamu(config, systemClock)
      .run(cliArgs)
  } catch {
    case e: UsageException =>
      logger.error(e.getMessage)
      sys.exit(1)
  }

}
