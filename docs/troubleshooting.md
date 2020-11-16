# Troubleshooting <!-- omit in toc -->

- [Standard Logging](#standard-logging)
- [Verbose Logging](#verbose-logging)
- [Engine Errors](#engine-errors)

## Standard Logging
When `kamu` runs it logs into `.kamu/run` directory. This directory is cleaned up on every run.

## Verbose Logging
When encountering an error that is not descriptive enough you may want to see more debugging information by using `kamu -v <command>` flag. This flag will redirect `kamu` log output into the terminal (while removing all UI widgets for clarity) and will enable backtrace information on errors. Please use this mode when submitting bugs on this repo.

## Engine Errors
In this early stages of development the error reporting from engines is not implemented yet, so you will definitely encounter some issues when crafting correct queries for the engines. Debugging them will require some ability to read exception information logged by the engines.

When `kamu` runs an engine it redirects its logs into `.kamu/run` directory. If you get a cryptic "engine operation failed" error you will need to inspect the logs in that directory for some clues on why your query failed.

Bear with us while we are working on improving the error reporting from the engines.
