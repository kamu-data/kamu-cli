---
name: kamu-github-operations
description: Working with GitHub from Kamu CLI sessions through the `gh` CLI — editing PR and issue titles, descriptions and labels, attaching images, and the known `gh` failures with their workarounds. Use before any `gh` command that changes a PR or issue, before adding screenshots to a PR, and whenever a `gh` command fails unexpectedly.
---

# GitHub Operations

Use the `gh` CLI. Anything that pushes, posts or edits on GitHub is outward-facing: it needs the
user's approval for that step ([AGENTS.md, "Hard rules"](../../../AGENTS.md#hard-rules)).

## Known failures and workarounds

| Symptom | Cause | Workaround |
|---|---|---|
| `gh pr edit` fails with `GraphQL: Projects (classic) is being deprecated … (repository.pullRequest.projectCards)` | `gh pr edit` reads the PR's classic project cards, which GitHub's API now refuses; the edit is never sent | Use the REST API, below |

### Editing a PR through the REST API

Write the new body to a file in the scratchpad, then send it with `-F body=@<file>`, which avoids
shell quoting of Markdown:

```bash
gh pr view <number> --json body -q .body > <scratchpad>/pr_body.md   # reading still works
# edit the file
gh api -X PATCH repos/kamu-data/kamu-cli/pulls/<number> -F body=@<scratchpad>/pr_body.md -q .html_url
```

The same endpoint takes `-f title=…`. Issues use `repos/<owner>/<repo>/issues/<number>`, which also
serves PRs for labels and assignees. Read the PR back afterwards to check the edit landed.

## Attaching images

Images in a PR description or comment are GitHub user attachments, the same as an image dragged
into the web editor. Never commit them, push a branch or open a PR to host them, or link them
through `raw.githubusercontent.com`: such branches outlive the PR and clutter the repository.

Upload each image, then embed the returned `url` as a Markdown image with alt text and update the
body through the REST API, as above:

```bash
curl -s -X POST "https://uploads.github.com/user-attachments/assets?name=shot.png&content_type=image/png&repository_id=$(gh api repos/kamu-data/kamu-cli --jq .id)" \
  -H "Authorization: Bearer $(gh auth token)" -H "Accept: application/json" \
  --data-binary @shot.png
```

`gh` 2.99 and later also take `--attach 'path/to/image.png#Alt text'` on `pr create`, `pr edit`
and `pr comment`, but `pr edit` still hits the classic Projects failure in this repository. The
upload route was proven in kamu-web-ui; the first use in kamu-cli should confirm it here.

## Adding to this skill

Add a row to the table when a `gh` command fails in a way that will recur, with the exact error
text so the next session can match it, and the workaround that was verified.
