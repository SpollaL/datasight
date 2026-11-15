#!/usr/bin/env bash
# Prepare a release from the Conventional Commit subjects since the last tag:
# bump Cargo.toml + Cargo.lock, write the CHANGELOG section, print the version.
# Prints nothing and touches nothing when no commit is a feat, fix, perf or
# breaking change. The release job in ci.yml commits, tags and pushes.
#
#   feat → Added, fix → Fixed, perf → Changed, type! → Changed (Breaking)
#   breaking → major (minor while 0.x), feat → minor, fix/perf → patch
set -euo pipefail

last=$(git describe --tags --abbrev=0 --match 'v[0-9]*')
current=$(sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml | head -n1)
if [ "v$current" != "$last" ]; then
  echo "Cargo.toml says $current but the last tag is $last" >&2
  exit 1
fi
if sed -n '/^## \[Unreleased\]/,/^## \[[0-9]/p' CHANGELOG.md | grep -q '^- '; then
  echo "CHANGELOG.md has entries under [Unreleased], which would be lost: the" >&2
  echo "changelog is generated from commit subjects, so put them there instead" >&2
  exit 1
fi

re='^([a-z]+)(\(([^)]+)\))?(!)?: (.+)$'
added='' changed='' fixed='' level=0
while IFS= read -r subject; do
  [[ $subject =~ $re ]] || { echo "skipped, not a Conventional Commit: $subject" >&2; continue; }
  type=${BASH_REMATCH[1]} scope=${BASH_REMATCH[3]} bang=${BASH_REMATCH[4]} text=${BASH_REMATCH[5]}
  [ -n "$scope" ] && text="$scope: $text"
  case $type$bang in
    *!)   changed+="- **Breaking:** $text"$'\n'; rank=3 ;;
    feat) added+="- $text"$'\n'; rank=2 ;;
    fix)  fixed+="- $text"$'\n'; rank=1 ;;
    perf) changed+="- $text"$'\n'; rank=1 ;;
    *)    continue ;;
  esac
  level=$((rank > level ? rank : level))
done < <(git log --no-merges --reverse --format=%s "$last..HEAD")

IFS=. read -r major minor patch <<<"$current"
((major == 0 && level == 3)) && level=2
case $level in
  0) exit 0 ;;
  1) next=$major.$minor.$((patch + 1)) ;;
  2) next=$major.$((minor + 1)).0 ;;
  3) next=$((major + 1)).0.0 ;;
esac

sed -i "s/^version = \"$current\"$/version = \"$next\"/" Cargo.toml
sed -i "/^name = \"datasight\"$/{n;s/^version = \"$current\"$/version = \"$next\"/}" Cargo.lock

section="## [$next] - $(date -u +%F)"$'\n'
[ -n "$added" ] && section+=$'\n### Added\n'"$added"
[ -n "$changed" ] && section+=$'\n### Changed\n'"$changed"
[ -n "$fixed" ] && section+=$'\n### Fixed\n'"$fixed"
# An empty [Unreleased] heading is dropped; the new section goes above the
# newest release. ENVIRON, not -v, so backslashes in subjects survive.
S=$section awk '
  /^## \[Unreleased\]/ { skip = 1; next }
  /^## \[/ { skip = 0; if (!done++) print ENVIRON["S"] }
  !skip
  END { if (!done) print ENVIRON["S"] }
' CHANGELOG.md >CHANGELOG.md.tmp
mv CHANGELOG.md.tmp CHANGELOG.md

echo "$next"
