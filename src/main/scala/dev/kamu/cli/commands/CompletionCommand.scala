/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.cli.commands

import dev.kamu.cli.{CliArgs, UsageException}
import org.apache.log4j.LogManager
import org.rogach.scallop.Scallop

class CompletionCommand(
  cliArgs: CliArgs,
  shellType: String
) extends Command {
  private val logger = LogManager.getLogger(getClass.getName)

  override def requiresWorkspace: Boolean = false

  def run(): Unit = {
    shellType match {
      case "bash" =>
        println(generateBashCompletion(cliArgs.builder))
      case _ =>
        throw new UsageException(s"Unsupported shell: $shellType")
    }
  }

  def generateBashCompletion(builder: Scallop): String = {
    Seq(
      generateBashCompletionFunc(List.empty, builder),
      "complete -F _kamu_ kamu"
    ).mkString("\n\n")
  }

  def generateBashCompletionFunc(
    path: Seq[String],
    builder: Scallop
  ): String = {
    val preamble = path match {
      case Seq() =>
        Seq(
          "local cur prev words cword",
          "_init_completion || return",
          "",
          "# Find workspace",
          "WORKSPACE_ROOT=`pwd`",
          "while : ; do",
          "  KAMU_ROOT=\"$WORKSPACE_ROOT/.kamu\"",
          "  [[ \"$WORKSPACE_ROOT\" != \"/\" ]] && [[ ! -d \"$KAMU_ROOT\" ]] || break",
          "  WORKSPACE_ROOT=`dirname \"$WORKSPACE_ROOT\"`",
          "done"
        )
      case _ =>
        Seq.empty
    }

    val subcommands = builder.subbuilders.map(_._1)

    val subcommandDispatches = subcommands
      .flatMap(
        c =>
          Seq(c + ")") ++ Seq(
            "_kamu_" + (path :+ c).mkString("_"),
            "return",
            ";;"
          ).tab
      )

    val dispatch =
      if (subcommands.isEmpty)
        Seq.empty
      else
        Seq(
          "case ${COMP_WORDS[COMP_CWORD-1]} in"
        ) ++ subcommandDispatches.tab ++ Seq(
          "*)".tab,
          ";;".tab.tab,
          "esac"
        )

    val flags = builder.opts.filter(!_.isPositional).map("--" + _.name)

    val positional = builder.opts
      .filter(_.isPositional)
      .flatMap(opt => {
        opt.name match {
          case "ids" if opt.descr.contains("dataset") =>
            Seq(
              "if [[ -d \"$KAMU_ROOT\" ]]; then",
              "  options+=`ls \"$KAMU_ROOT/datasets\"`",
              "fi"
            )
          case "ids" if opt.descr.contains("volume") =>
            Seq(
              "if [[ -d \"$KAMU_ROOT\" ]]; then",
              "  options+=`ls \"$KAMU_ROOT/volumes\" | sed -e 's/\\.[^.]*$//'`",
              "fi"
            )
          case _ =>
            Seq.empty
        }
      })

    val finale = Seq(
      "if [[ $cur == -* ]]; then",
      "  COMPREPLY=( $(compgen -W \"${flags[*]}\" -- ${cur}) )",
      "else",
      "  COMPREPLY=( $(compgen -W \"${options[*]}\" -- ${cur}) )",
      "  if [ ${#COMPREPLY[@]} -eq 0 ]; then",
      "    _filedir",
      "  fi",
      "fi"
    )

    val body = (
      preamble.break
        ++ dispatch.break
        ++ Seq("options=()")
        ++ subcommands
          .map(c => "options+=(\"" + c + "\")")
          .break
        ++ Seq("flags=(\"--help\")")
        ++ flags
          .map(f => "flags+=(\"" + f + "\")")
          .break
        ++ positional.break
        ++ finale
    ).tab

    val ownDef = (Seq(
      s"_kamu_${path.mkString("_")}() {"
    ) ++ body ++ Seq(
      "}"
    )).mkString("\n")

    val subcommandsDefs = builder.subbuilders.map({
      case (name, b) => generateBashCompletionFunc(path :+ name, b)
    })

    (subcommandsDefs :+ ownDef).mkString("\n\n")
  }

  implicit class CodeLine(val l: String) {
    def tab: String = "  " + l
  }

  implicit class CodeBlock(val block: Seq[String]) {
    def tab: Seq[String] = block.map("  " + _)
    def break: Seq[String] = if (block.isEmpty) block else block :+ ""
  }

}
