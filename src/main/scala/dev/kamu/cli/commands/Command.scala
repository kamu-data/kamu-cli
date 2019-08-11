package dev.kamu.cli.commands

trait Command {
  def requiresRepository = true

  def run(): Unit
}
