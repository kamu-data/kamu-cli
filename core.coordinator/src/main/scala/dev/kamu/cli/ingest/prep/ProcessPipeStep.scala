/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.ingest.prep

import java.io.{IOException, InputStream, PipedInputStream, PipedOutputStream}

import org.apache.commons.io.IOUtils

import scala.sys.process.{Process, ProcessIO}

class ProcessPipeStep(
  command: Vector[String]
) extends PrepStep {
  private var process: Process = _

  override def prepare(inputStream: InputStream): InputStream = {
    logger.debug("Executing command: " + command.mkString(" "))

    val resultStream = new PipedInputStream()
    val resultOutputStream = new PipedOutputStream(resultStream)

    process = Process(command).run(
      new ProcessIO(
        stdin => {
          try {
            IOUtils.copy(inputStream, stdin)
            stdin.close()
          } catch {
            case e: IOException =>
              logger.error("Error communicating with the process", e)
          }
        },
        stdout => {
          try {
            IOUtils.copy(stdout, resultOutputStream)
          } finally {
            resultOutputStream.close()
          }
        },
        stderr => {
          scala.io.Source
            .fromInputStream(stderr)
            .getLines()
            .foreach(l => System.err.println("[subprocess] " + l))
        },
        true
      )
    )

    resultStream
  }

  override def join(): Unit = {
    val exitCode = process.exitValue()
    process = null

    if (exitCode != 0)
      throw new RuntimeException(
        s"Command returned non-zero exit code $exitCode: " +
          command.mkString(" ")
      )
  }
}
