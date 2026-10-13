---
name: kamu-github-operations
description: Working with GitHub from Kamu CLI sessions through the `gh` CLI — editing PR and issue titles, descriptions and labels, attaching images, and the known `gh` failures with their workarounds. Use before any `gh` command that changes a PR or issue, before adding screenshots to a PR, and whenever a `gh` command fails unexpectedly.
---

# GitHub Operations

Use the `gh` CLI. Pushing needs the user's approval for that step
([AGENTS.md, "Hard rules"](../../../AGENTS.md#hard-rules)). Editing a PR or issue other than the
one the session is working on (a merged PR, someone else's issue) is outward-facing too: ask first.

## Known failures and workarounds

| Symptom | Cause | Workaround |
|---|---|---|
| `gh pr edit` fails with `GraphQL: Projects (classic) is being deprecated … (repository.pullRequest.projectCards)` | `gh pr edit` reads the PR's classic project cards, which GitHub's API now refuses; the edit is never sent | Use the REST API, below. `gh pr create` and `gh pr view` are not affected |

### Editing a PR through the REST API

Write the new body to a file in the scratchpad, then send it with `-F body=@<file>`, which avoids
shell quoting of Markdown:

```bash
gh pr view <number> --json body -q .body > <scratchpad>/pr_body.md   # reading still works
# edit the file
gh api -X PATCH repos/kamu-data/kamu-cli/pulls/<number> -F body=@<scratchpad>/pr_body.md -q .body
```

The same endpoint takes `-f title=…`. Issues use `repos/<owner>/<repo>/issues/<number>`, which also
serves PRs for labels and assignees. `-q .body` prints the body GitHub stored, so the edit is
checked in the same call.

## Attaching images

Images in a PR description or comment are GitHub user attachments, the same as an image dragged
into the web editor. Never commit them, push a branch or open a PR to host them, host them in
a gist, or link them through `raw.githubusercontent.com`: such hosts outlive the PR and clutter the repository.

Upload each image; the call answers `201` with `{"url": "https://github.com/user-attachments/assets/…"}`:

```bash
curl -s -X POST "https://uploads.github.com/user-attachments/assets?name=shot.png&content_type=image/png&repository_id=$(gh api repos/kamu-data/kamu-cli --jq .id)" \
  -H "Authorization: Bearer $(gh auth token)" -H "Accept: application/json" \
  --data-binary @shot.png
```

The upload needs only the repository, not the PR, so upload first and put each `url` into the body
file as a Markdown image with alt text before `gh pr create --body-file`. For an existing PR,
update the body through the REST API, above. To replace a screenshot, upload the new one and
swap the URL in the body.

`gh` 2.99 and later also take `--attach 'path/to/image.png#Alt text'` on `pr create`, `pr edit`
and `pr comment`, but `pr edit` still hits the classic Projects failure. The upload route is the
verified one; it works the same for any repository.

## Adding to this skill

Add a row to the table when a `gh` command fails in a way that will recur, with the exact error
text so the next session can match it, and the workaround that was verified.
