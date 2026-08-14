#!/usr/bin/env bash
# usage: bash release.sh 0.1.2
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: bash release.sh <version>  (e.g. 0.3.1)" >&2
  exit 1
fi
VERSION="$1"

if [ -n "$(git status --porcelain)" ]; then
  echo "working directory not clean; commit or stash first:" >&2
  git status --short >&2
  exit 1
fi

git checkout master
git pull

# npm version bumps package.json, commits it, and tags v<version>
npm version "$VERSION"

echo "pushing tag v$VERSION..."
git push origin "v$VERSION"
git push

echo "released $VERSION"
