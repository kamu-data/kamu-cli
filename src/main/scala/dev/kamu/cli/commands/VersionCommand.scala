package dev.kamu.cli.commands

class VersionCommand() extends Command {
  override def requiresWorkspace: Boolean = false

  def run(): Unit = {
    println(getClass.getPackage.getImplementationVersion)
  }
}
