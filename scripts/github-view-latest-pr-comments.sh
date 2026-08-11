#!/usr/bin/env bash

set -euo pipefail

# Show all PR comments (general + review + inline) as Markdown,
# sorted from newest to oldest, with links to the threads.
#
# Dependencies: gh (gh auth login), jq.
#
# Usage:
#   ./github-view-latest-pr-comments.sh [--all] <owner> <repo> <pr_number> > comments.md
#
# Example:
#   ./github-view-latest-pr-comments.sh kamu-data kamu-cli 1674 > pr-1674-open-comments.md
#   ./github-view-latest-pr-comments.sh kamu-data kamu-cli 1674 --all > pr-1674-all-comments.md

SHOW_ALL=0
ARGS=()
for arg in "$@"; do
  case "$arg" in
    --all)
      SHOW_ALL=1
      ;;
    -*)
      echo "Unknown option: $arg" >&2
      echo "Usage: $0 [--all] <owner> <repo> <pr_number>" >&2
      exit 1
      ;;
    *)
      ARGS+=("$arg")
      ;;
  esac
done

OWNER="${ARGS[0]:?GitHub org/owner required: e.g. kamu-data}"
REPO="${ARGS[1]:?GitHub repo required: e.g. kamu-cli}"
PR="${ARGS[2]:?PR number required}"

TMP=$(mktemp)
trap 'rm -f "$TMP"' EXIT

{
  # 1) General comments
  gh api "repos/$OWNER/$REPO/issues/$PR/comments" --paginate \
    --jq '.[] | {
      date: .created_at,
      author: .user.login,
      url: .html_url,
      kind: "comment",
      path: null,
      line: null,
      resolved: null,
      body: .body
    }'

  # 2) Inline review-comments
  # shellcheck disable=SC2016
  gh api graphql --paginate \
    -F owner="$OWNER" -F repo="$REPO" -F pr="$PR" \
    -f query='
      query($owner: String!, $repo: String!, $pr: Int!, $endCursor: String) {
        repository(owner: $owner, name: $repo) {
          pullRequest(number: $pr) {
            reviewThreads(first: 100, after: $endCursor) {
              pageInfo { hasNextPage endCursor }
              nodes {
                isResolved
                path
                comments(first: 100) {
                  nodes {
                    url
                    createdAt
                    line
                    originalLine
                    body
                    author { login }
                  }
                }
              }
            }
          }
        }
      }
    ' \
    --jq '.data.repository.pullRequest.reviewThreads.nodes[] | . as $t |
      $t.comments.nodes[] | {
        date: .createdAt,
        author: .author.login,
        url: .url,
        kind: "review-comment",
        path: $t.path,
        line: (.line // .originalLine),
        resolved: $t.isResolved,
        body: .body
      }'

  # 3) Review itself (approve/changes requested/summary)
  gh api "repos/$OWNER/$REPO/pulls/$PR/reviews" --paginate \
    --jq '.[] | select(.body != "") | {
      date: .submitted_at,
      author: .user.login,
      url: .html_url,
      kind: "review",
      path: null,
      line: null,
      resolved: null,
      body: .body
    }'
} >> "$TMP"

echo "# PR comments for [$OWNER/$REPO#$PR](https://github.com/$OWNER/$REPO/pull/$PR)"
echo

# Output comments desc, example:
#
# ```md
# ### 2026-08-10T19:39:59Z — s373r (review-comment)
# - Thread status: 🟢 Resolved
# - Local path: [src/domain/accounts/domain/src/services/authentication_provider.rs:33](src/domain/accounts/domain/src/services/authentication_provider.rs)
# - GitHub link: https://github.com/kamu-data/kamu-cli/pull/1674#discussion_r3752782360
#
# Not actual anymore
#```
jq -s --argjson show_all "$SHOW_ALL" '
    sort_by(.date) | reverse | map(select($show_all == 1 or .resolved != true)) | .[]
  ' "$TMP" \
  | jq -r '
    "### " + .date + " — " + .author + " (" + .kind + ")\n" +
    (if .resolved != null then
      "- Thread status: " + (if .resolved then "🟢 Resolved" else "🔴 Open" end) + "\n"
    else
      ""
    end) +
    (if .path then
      "- Local path: [" + .path + (if .line then ":" + (.line|tostring) else "" end) + "](" + .path + ")\n"
    else
      ""
    end) +
    "- Thread link: " + .url + "\n" +
    "\n" +
    .body + "\n" +
    "\n" +
    "---\n"
  '
