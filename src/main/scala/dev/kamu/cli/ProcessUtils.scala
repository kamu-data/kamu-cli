package dev.kamu.cli

import sun.misc.Signal

import scala.sys.process.Process

object ProcessUtils {

  implicit class ProcessExt(val p: Process) {
    def pid: Long = {
      val procField = p.getClass.getDeclaredField("p")
      procField.synchronized {
        procField.setAccessible(true)
        val proc = procField.get(p)
        try {
          proc match {
            case unixProc
                if unixProc.getClass.getName == "java.lang.UNIXProcess" =>
              val pidField = unixProc.getClass.getDeclaredField("pid")
              pidField.synchronized {
                pidField.setAccessible(true)
                try {
                  pidField.getLong(unixProc)
                } finally {
                  pidField.setAccessible(false)
                }
              }
            case _ =>
              throw new RuntimeException(
                "Cannot get PID of a " + proc.getClass.getName
              )
          }
        } finally {
          procField.setAccessible(false)
        }
      }
    }

    def kill(signal: Signal): Unit = {
      Runtime.getRuntime.exec(s"kill -${signal.getNumber} ${p.pid}")
    }
  }

}
