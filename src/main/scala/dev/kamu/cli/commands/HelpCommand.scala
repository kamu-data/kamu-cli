package dev.kamu.cli.commands

import dev.kamu.cli.CliArgs

class HelpCommand(
  cliArgs: CliArgs
) extends Command {
  override def requiresRepository: Boolean = false

  def run(): Unit = {
    cliArgs.printHelp()
  }
}
