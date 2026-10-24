#!/bin/sh
# Git-flow release: merge develop into master (no squash), tag master with the next version, merge master back into
# develop, then push master, develop and the tag in one atomic push. The tag pipeline does the actual publishing.
#
# Usage: ./release.sh patch|minor|major
#   REMOTE=origin        remote to release on
#   RELEASE_REF=<rev>    commit to release (default: the remote's develop tip); CI passes the tested commit
#   DRY_RUN=1            do everything locally, but don't push
set -eu

BUMP="${1:-}"
REMOTE="${REMOTE:-origin}"

case "$BUMP" in
    patch|minor|major) ;;
    *) echo "usage: $0 patch|minor|major" >&2; exit 1 ;;
esac

if [ -n "$(git status --porcelain)" ]; then
    echo "Working tree is not clean, aborting." >&2
    exit 1
fi

git fetch --tags "$REMOTE" \
    "+refs/heads/master:refs/remotes/$REMOTE/master" \
    "+refs/heads/develop:refs/remotes/$REMOTE/develop"
RELEASE_REF="${RELEASE_REF:-$REMOTE/develop}"

# Local master/develop get reset to the remote's; refuse if that would drop unpushed commits.
for BRANCH in master develop; do
    if git rev-parse -q --verify "refs/heads/$BRANCH" >/dev/null \
        && ! git merge-base --is-ancestor "$BRANCH" "$REMOTE/$BRANCH"; then
        echo "Local $BRANCH has commits not on $REMOTE/$BRANCH, aborting." >&2
        exit 1
    fi
done

# Latest plain X.Y.Z tag; pre-release tags like 0.18.0-dev.49 are skipped.
LAST=$(git tag --list --sort=-v:refname | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' | head -n 1)
LAST="${LAST:-0.0.0}"
IFS=. read -r MAJOR MINOR PATCH <<EOF
$LAST
EOF
case "$BUMP" in
    major) VERSION="$((MAJOR + 1)).0.0" ;;
    minor) VERSION="$MAJOR.$((MINOR + 1)).0" ;;
    patch) VERSION="$MAJOR.$MINOR.$((PATCH + 1))" ;;
esac

# Compare trees, not ancestry: right after a release develop holds only the merge-back commit.
if git diff --quiet "$REMOTE/master" "$RELEASE_REF"; then
    echo "Nothing to release: $RELEASE_REF has no changes compared to master." >&2
    exit 1
fi

echo "Releasing $RELEASE_REF as $VERSION (previous: $LAST)"

git checkout -B master "$REMOTE/master"
git merge -q --no-ff -m "Merge develop into master for $VERSION" "$RELEASE_REF"
git tag -a "$VERSION" -m "Release $VERSION"

git checkout -B develop "$REMOTE/develop"
git merge -q --no-ff -m "Merge master back into develop after $VERSION" master

if [ "${DRY_RUN:-}" = "1" ]; then
    echo "DRY_RUN: not pushing. Would run: git push --atomic $REMOTE master develop $VERSION"
    exit 0
fi

# Atomic: if develop or master moved on the remote meanwhile, nothing is pushed.
git push --atomic "$REMOTE" master develop "refs/tags/$VERSION"
echo "Released $VERSION"
