package dev.kamu.cli.commands

trait Command {
  def requiresWorkspace = true

  def run(): Unit
}
