// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use clap::CommandFactory as _;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// One kind of candidate to emit. A cursor position can yield several.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Completion {
    Subcommands,
    /// Keyed by the option's clap `value_name`.
    OptionValue(String),
    /// Keyed by the option's clap id.
    OptionPossibleValues(String),
    Positional(PositionalKind),
    OptionNames,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The domain behind a positional, resolved from its
/// (command path, clap positional id) pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionalKind {
    Alias,
    ConfigKey,
    Dataset,
    Path,
    Repository,
    ResourceType { with_extra_targets: bool },
    ContextName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct CompletionPlan {
    pub completions: Vec<Completion>,
    /// The prefix every candidate must match.
    pub to_complete: String,
}

impl CompletionPlan {
    fn nothing() -> Self {
        Self {
            completions: Vec::new(),
            to_complete: String::new(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Decides what to complete for `args` with the cursor on token `current`
/// (bash's `COMP_WORDS` and `COMP_CWORD`). `current == args.len()` means a
/// fresh word after a trailing space.
pub fn plan_completion(args: &[String], current: usize) -> CompletionPlan {
    // Past the end there is no token to anchor to.
    if current > args.len() {
        return CompletionPlan::nothing();
    }

    let cli = crate::cli::Cli::command();
    let args = &args[..(current + 1).min(args.len())];

    let empty = String::new();
    // `current` can be 0, so this must not underflow.
    let prev = current
        .checked_sub(1)
        .and_then(|i| args.get(i))
        .unwrap_or(&empty);
    let to_complete = args.get(current).unwrap_or(&empty);

    let walk = walk_to_active_command(&cli, args, current);
    let last_cmd = walk.last_cmd;

    let mut completions = Vec::new();

    // An option's value replaces every other candidate.
    if prev.starts_with('-')
        && let Some(opt) = find_option(last_cmd, prev)
        && opt.get_action().takes_values()
    {
        if let Some(value_names) = opt.get_value_names() {
            for name in value_names {
                completions.push(Completion::OptionValue(name.as_str().to_owned()));
            }
        }
        completions.push(Completion::OptionPossibleValues(
            opt.get_id().as_str().to_owned(),
        ));

        return CompletionPlan {
            completions,
            to_complete: to_complete.clone(),
        };
    }

    completions.push(Completion::Subcommands);

    for pos in last_cmd.get_positionals() {
        if let Some(kind) = classify_positional(
            &walk.path,
            pos.get_id().as_str(),
            walk.free_positionals_before_cursor,
        ) {
            completions.push(Completion::Positional(kind));
        }
    }

    if to_complete.starts_with('-') {
        completions.push(Completion::OptionNames);
    }

    CompletionPlan {
        completions,
        to_complete: to_complete.clone(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The subcommand the cursor sits in, for callers needing the `clap::Command`
/// itself rather than the completion decision.
pub fn resolve_active_command<'a>(
    cli: &'a clap::Command,
    args: &[String],
    current: usize,
) -> &'a clap::Command {
    let args = &args[..current.saturating_add(1).min(args.len())];
    walk_to_active_command(cli, args, current).last_cmd
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct CommandWalk<'a> {
    last_cmd: &'a clap::Command,
    /// Matched subcommand names, outermost first, distinguishing
    /// `context delete` from the top-level `delete`.
    path: Vec<String>,
    /// Free tokens already consumed: `kamu get <type> <name>...` accepts a
    /// type selector only in the first slot.
    free_positionals_before_cursor: usize,
}

/// Walks the tokens before the cursor to the active subcommand, skipping
/// options and their values so neither halts discovery nor counts as a
/// positional.
fn walk_to_active_command<'a>(
    cli: &'a clap::Command,
    args: &[String],
    current: usize,
) -> CommandWalk<'a> {
    let mut last_cmd = cli;
    let mut path = Vec::new();
    let mut free_positionals_before_cursor = 0;
    let mut expecting_option_value = false;

    for arg in args.iter().take(current).skip(1) {
        if std::mem::take(&mut expecting_option_value) {
            continue;
        }

        if let Some(opt) = find_option(last_cmd, arg) {
            expecting_option_value = opt.get_action().takes_values();
            continue;
        }

        // An unrecognised flag, or one with its value attached
        // (`--context=prod`, `-cprod`): not a positional, nothing pending.
        if arg.starts_with('-') {
            continue;
        }

        match last_cmd
            .get_subcommands()
            .find(|s| s.get_name() == arg.as_str() || s.get_visible_aliases().any(|a| a == arg))
        {
            Some(sub) => {
                last_cmd = sub;
                path.push(sub.get_name().to_owned());
            }
            None => free_positionals_before_cursor += 1,
        }
    }

    CommandWalk {
        last_cmd,
        path,
        free_positionals_before_cursor,
    }
}

/// The option a whole token names: `--long` or `-s`, exactly. Attached
/// spellings carry their own value, so they must not match.
fn find_option<'a>(cmd: &'a clap::Command, token: &str) -> Option<&'a clap::Arg> {
    if let Some(long) = token.strip_prefix("--") {
        if long.is_empty() || long.contains('=') {
            return None;
        }
        return cmd.get_opts().find(|o| o.get_long() == Some(long));
    }

    let short = token.strip_prefix('-')?;
    let mut chars = short.chars();
    let c = chars.next()?;
    if chars.next().is_some() {
        return None;
    }
    cmd.get_opts().find(|o| o.get_short() == Some(c))
}

/// Maps a (command path, positional id) pair to the domain behind it.
/// Matching the path, not the bare id, keeps context-name completion off the
/// other commands spelling a positional `name`.
fn classify_positional(
    path: &[String],
    positional_id: &str,
    free_positionals_before_cursor: usize,
) -> Option<PositionalKind> {
    let path: Vec<&str> = path.iter().map(String::as_str).collect();
    let first_slot = free_positionals_before_cursor == 0;

    let kind = match (path.as_slice(), positional_id) {
        // `get`/`delete` take a selector only in the first slot; every `list`
        // slot is one.
        (["delete"], "target") if first_slot => PositionalKind::ResourceType {
            with_extra_targets: true,
        },
        (["list"], "targets") => PositionalKind::ResourceType {
            with_extra_targets: true,
        },
        (["get"], "args") if first_slot => PositionalKind::ResourceType {
            with_extra_targets: false,
        },

        // Commands taking an existing context name; `context add` takes a new
        // one and is absent by construction.
        (["context"] | ["context", "use" | "check" | "delete"], "name") => {
            PositionalKind::ContextName
        }

        (_, "alias") => PositionalKind::Alias,
        (_, "cfgkey") => PositionalKind::ConfigKey,
        (_, "dataset") => PositionalKind::Dataset,
        (_, "file" | "manifest") => PositionalKind::Path,
        (_, "repository") => PositionalKind::Repository,

        _ => return None,
    };

    Some(kind)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn plan(line: &str, current: usize) -> CompletionPlan {
        let args: Vec<String> = line.split_whitespace().map(ToOwned::to_owned).collect();
        plan_completion(&args, current)
    }

    fn positionals(line: &str, current: usize) -> Vec<PositionalKind> {
        plan(line, current)
            .completions
            .into_iter()
            .filter_map(|c| match c {
                Completion::Positional(kind) => Some(kind),
                _ => None,
            })
            .collect()
    }

    fn option_values(line: &str, current: usize) -> Vec<String> {
        plan(line, current)
            .completions
            .into_iter()
            .filter_map(|c| match c {
                Completion::OptionValue(name) => Some(name),
                _ => None,
            })
            .collect()
    }

    const RESOURCE_TYPE_WITH_EXTRAS: PositionalKind = PositionalKind::ResourceType {
        with_extra_targets: true,
    };
    const RESOURCE_TYPE_BARE: PositionalKind = PositionalKind::ResourceType {
        with_extra_targets: false,
    };

    #[test]
    fn resource_type_completes_for_get_list_delete() {
        assert!(positionals("kamu get", 2).contains(&RESOURCE_TYPE_BARE));
        assert!(positionals("kamu list", 2).contains(&RESOURCE_TYPE_WITH_EXTRAS));
        assert!(positionals("kamu delete", 2).contains(&RESOURCE_TYPE_WITH_EXTRAS));
    }

    #[test]
    fn resource_type_completes_only_for_the_first_get_token() {
        // `kamu get <type> <name>...` - the second token is a name.
        assert!(!positionals("kamu get vs my-vars", 3).contains(&RESOURCE_TYPE_BARE));
    }

    #[test]
    fn resource_type_completes_only_for_the_first_delete_token() {
        assert!(!positionals("kamu delete variablesets d", 3).contains(&RESOURCE_TYPE_WITH_EXTRAS));
    }

    #[test]
    fn every_list_slot_is_a_resource_type() {
        // `List.targets` is `num_args = 0..`, so later slots stay selectors.
        assert!(positionals("kamu list vs/app-%", 3).contains(&RESOURCE_TYPE_WITH_EXTRAS));
    }

    #[test]
    fn a_preceding_flag_does_not_consume_a_positional_slot() {
        // `--spec` is a flag, not a positional: `get` is still on slot 0.
        assert!(positionals("kamu get --spec", 3).contains(&RESOURCE_TYPE_BARE));
    }

    #[test]
    fn a_leading_global_flag_does_not_stop_subcommand_discovery() {
        assert!(positionals("kamu -v get", 3).contains(&RESOURCE_TYPE_BARE));
    }

    #[test]
    fn an_option_and_its_value_consume_no_positional_slot() {
        assert!(positionals("kamu list --context prod", 4).contains(&RESOURCE_TYPE_WITH_EXTRAS));
    }

    #[test]
    fn an_attached_option_value_consumes_no_positional_slot() {
        // clap accepts `--context=prod`, `-cprod` and the clustered `-wcprod`
        // (== `-w -c prod`). We do not *complete* those spellings, but they
        // must not be miscounted as positionals when already typed.
        for line in [
            "kamu list --context=prod",
            "kamu list -cprod",
            "kamu list -wcprod",
        ] {
            assert!(
                positionals(line, 3).contains(&RESOURCE_TYPE_WITH_EXTRAS),
                "attached option value miscounted in: {line}"
            );
        }
    }

    #[test]
    fn an_option_value_spelling_a_subcommand_name_is_not_followed() {
        // `get` here is the value of `--context`, not a subcommand hop, so the
        // active command is still `list` and its own positional applies.
        assert!(positionals("kamu list --context get", 4).contains(&RESOURCE_TYPE_WITH_EXTRAS));

        // And the value must not be mistaken for a positional slot either.
        assert!(positionals("kamu get --context vs", 4).contains(&RESOURCE_TYPE_BARE));
    }

    #[test]
    fn context_name_completes_for_context_and_its_existing_name_subcommands() {
        assert!(positionals("kamu context", 2).contains(&PositionalKind::ContextName));
        for sub in ["use", "check", "delete"] {
            let line = format!("kamu context {sub}");
            assert!(
                positionals(&line, 3).contains(&PositionalKind::ContextName),
                "no context-name completion for: {line}"
            );
        }
    }

    #[test]
    fn context_add_takes_a_new_name_and_suggests_nothing() {
        assert!(!positionals("kamu context add", 3).contains(&PositionalKind::ContextName));
    }

    #[test]
    fn top_level_delete_does_not_suggest_context_names() {
        // `delete` exists both top-level and under `context`; only the latter
        // takes an existing context name.
        assert!(!positionals("kamu delete", 2).contains(&PositionalKind::ContextName));
    }

    #[test]
    fn commands_taking_a_new_name_do_not_suggest_context_names() {
        for (line, current) in [
            ("kamu new", 2),
            ("kamu rename foo.bar", 3),
            ("kamu repo add", 3),
        ] {
            assert!(
                !positionals(line, current).contains(&PositionalKind::ContextName),
                "unexpected context-name completion for: {line}"
            );
        }
    }

    #[test]
    fn context_option_value_completes_context_names() {
        for line in ["kamu list --context", "kamu list -c"] {
            assert_eq!(
                option_values(line, 3),
                vec!["CONTEXT_NAME".to_string()],
                "no context-name option value for: {line}"
            );
        }
    }

    #[test]
    fn pull_as_takes_a_dataset_name_not_a_context_name() {
        assert_eq!(
            option_values("kamu pull foo.bar --as", 4),
            vec!["DATASET_NAME".to_string()]
        );
    }

    #[test]
    fn baseline_positionals_still_resolve() {
        assert!(positionals("kamu log", 2).contains(&PositionalKind::Dataset));
        assert!(positionals("kamu config set engine.runt", 3).contains(&PositionalKind::ConfigKey));
        assert!(positionals("kamu apply", 2).contains(&PositionalKind::Path));
    }

    #[test]
    fn subcommands_are_offered_when_no_option_value_is_pending() {
        assert!(
            plan("kamu l", 1)
                .completions
                .contains(&Completion::Subcommands)
        );
    }

    #[test]
    fn an_option_value_suppresses_every_other_candidate() {
        let completions = plan("kamu list --output-format", 3).completions;
        assert!(!completions.contains(&Completion::Subcommands));
        assert!(
            !completions
                .iter()
                .any(|c| matches!(c, Completion::Positional(_)))
        );
    }

    #[test]
    fn option_names_are_offered_only_for_a_dash_led_token() {
        assert!(
            plan("kamu list -", 2)
                .completions
                .contains(&Completion::OptionNames)
        );
        assert!(
            !plan("kamu list", 2)
                .completions
                .contains(&Completion::OptionNames)
        );
    }

    #[test]
    fn a_zero_cursor_does_not_panic() {
        assert!(
            plan("kamu", 0)
                .completions
                .contains(&Completion::Subcommands)
        );
    }

    #[test]
    fn an_out_of_range_cursor_yields_nothing() {
        assert!(plan("kamu get", 99).completions.is_empty());
    }
}
