package dev.kamu.cli.commands

import dev.kamu.cli.CliParser

class HelpCommand(
  cliParser: CliParser
) extends Command {
  override def requiresRepository: Boolean = false

  def run(): Unit = {
    println(cliParser.usage())
  }
}
