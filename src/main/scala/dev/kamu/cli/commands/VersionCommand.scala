package dev.kamu.cli.commands

class VersionCommand() extends Command {
  override def requiresRepository: Boolean = false

  def run(): Unit = {
    println(getClass.getPackage.getImplementationVersion)
  }
}
