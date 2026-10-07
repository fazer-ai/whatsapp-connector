#!/usr/bin/env bash
# Builds third_party/meowcaller from upstream and checks it, see third_party/README.md.
#
#   meowcaller.sh sync    rewrite third_party/meowcaller from the pin plus the patches
#   meowcaller.sh check   rebuild it elsewhere and fail when it differs from the tree
#   meowcaller.sh watch   compare the pin with upstream and open or update an issue
#
# The copy is derived, never edited: the pin and the patches are the source of truth,
# and `check` is what makes a hand edit to the copy fail instead of quietly becoming
# the version everybody builds.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
copy="$root/third_party/meowcaller"
patches="$root/third_party/patches/meowcaller"
upstream_repo="purpshell/meowcaller"
upstream_url="https://github.com/$upstream_repo"
watch_title="o meowcaller andou"

# shellcheck source=meowcaller.pin
. "$root/third_party/meowcaller.pin"

# build writes upstream at COMMIT, with the import rewrite and the patches applied, to $1.
build() {
	local out=$1
	mkdir -p "$out"
	git -C "$out" init -q
	git -C "$out" fetch -q --depth 1 "$upstream_url" "$COMMIT"
	git -C "$out" checkout -q FETCH_HEAD
	rm -rf "$out/.git"

	# Upstream builds against a whatsmeow fork with the same packages; this repository
	# builds against whatsmeow itself. The rewrite is a path and nothing else (upstream's
	# own #33 says the same of the fork), so it is done here rather than kept as a patch
	# touching every file. The require line it leaves names a fork version that whatsmeow
	# does not have, and the first patch is what sets it to this repository's pin.
	grep -rlF 'github.com/polymorfa/hypermeow' "$out" | while IFS= read -r f; do
		perl -pi -e 's#github\.com/polymorfa/hypermeow#go.mau.fi/whatsmeow#g' "$f"
	done

	local p
	for p in "$patches"/*.patch; do
		[ -e "$p" ] || continue
		if ! git -C "$out" apply --whitespace=nowarn "$p"; then
			echo "meowcaller: $(basename "$p") does not apply on $COMMIT" >&2
			echo "upstream changed the code it touches: read the issue the patch names before rewriting it" >&2
			return 1
		fi
	done

	# Upstream's tests stay upstream. Nothing here runs their suite, and its crypto
	# known-answer vectors (SRTP and STUN keys in testdata/kats.json) read as secrets to
	# the push guard of a public repository. What builds is all kept.
	find "$out" -name '*_test.go' -type f -delete
	find "$out" -name testdata -type d -prune -exec rm -rf {} +
}

cmd_sync() {
	tmp=$(mktemp -d)
	trap 'rm -rf "$tmp"' EXIT
	build "$tmp/src"
	rm -rf "$copy"
	mv "$tmp/src" "$copy"
	echo "third_party/meowcaller rebuilt from $COMMIT plus $(find "$patches" -name '*.patch' | wc -l | tr -d ' ') patch(es)"
}

cmd_check() {
	tmp=$(mktemp -d)
	trap 'rm -rf "$tmp"' EXIT
	build "$tmp/src"
	# Against what is committed, not only the working tree: an edit can reach a commit
	# without ever being in a working tree this runs on, and a file the copy carries but
	# git does not (an ignore rule matching upstream) is a copy CI never sees.
	local untracked
	untracked=$(git -C "$root" ls-files --others -- third_party/meowcaller)
	if [ -n "$untracked" ]; then
		echo "meowcaller: files in third_party/meowcaller that git does not track:" >&2
		echo "$untracked" >&2
		return 1
	fi
	if ! diff -r "$tmp/src" "$copy" >"$tmp/diff"; then
		echo "meowcaller: third_party/meowcaller differs from $COMMIT plus third_party/patches/meowcaller:" >&2
		head -n 60 "$tmp/diff" >&2
		echo >&2
		echo "the copy is derived: put the change in a patch and run make meowcaller" >&2
		return 1
	fi
	echo "third_party/meowcaller matches $COMMIT plus its patches"
}

# watch_body prints what the issue says for the current upstream state. It has no clock
# in it on purpose: a body that only changes when upstream does is what lets a run with
# nothing new leave the issue alone.
watch_body() {
	local main=$1 pr_state=$2 pr_head=$3
	echo "<!-- meowcaller-upstream-watch -->"
	echo "O \`third_party/meowcaller\` está fixado em \`$COMMIT\` e o upstream andou. Este aviso é aberto pelo workflow semanal \`meowcaller-upstream\` e atualizado enquanto a diferença existir."
	echo
	if [ "$main" != "$COMMIT" ]; then
		echo "## \`main\` do $upstream_repo"
		echo
		echo "Fixado \`$COMMIT\`, \`main\` em \`$main\`: $upstream_url/compare/$COMMIT...$main"
		echo
		gh api "repos/$upstream_repo/compare/$COMMIT...$main" \
			--jq '"Commits à frente: \(.ahead_by), atrás: \(.behind_by).\n", (.commits[] | "- `\(.sha[0:10])` \(.commit.message | split("\n")[0])")'
		echo
	fi
	echo "## PR #$PR do $upstream_repo"
	echo
	echo "Estado: \`$pr_state\`. Head usado no patch: \`$PR_HEAD\`; head agora: \`$pr_head\`. $upstream_url/pull/$PR"
	echo
	case "$pr_state" in
	merged) echo "Mergeada: no bump, o patch dela sai de \`third_party/patches/meowcaller\`." ;;
	closed) echo "Fechada sem merge: o patch dela continua sendo nosso, e vale ler por que foi fechada." ;;
	*) if [ "$pr_head" != "$PR_HEAD" ]; then echo "A PR ganhou commits: compare o diff novo com o nosso patch."; else echo "Sem mudança desde o patch."; fi ;;
	esac
	echo
	echo "Para atualizar: troque \`COMMIT\` em \`third_party/meowcaller.pin\`, rode \`make meowcaller\` e ajuste o patch que não aplicar."
}

cmd_watch() {
	local main pr_state pr_head
	main=$(gh api "repos/$upstream_repo/commits/main" --jq .sha)
	pr_state=$(gh api "repos/$upstream_repo/pulls/$PR" --jq 'if .merged then "merged" else .state end')
	pr_head=$(gh api "repos/$upstream_repo/pulls/$PR" --jq .head.sha)

	local existing
	# The plain list rather than search: search indexes a new issue with a delay, and a
	# second run inside that window opened a duplicate when this read it.
	existing=$(gh issue list --state open --limit 1000 --json number,title \
		--jq "[.[] | select(.title == \"$watch_title\")][0].number // empty")

	if [ "$main" = "$COMMIT" ] && [ "$pr_state" = "open" ] && [ "$pr_head" = "$PR_HEAD" ]; then
		echo "meowcaller: pin $COMMIT is upstream main and PR #$PR is unchanged"
		return 0
	fi

	local body
	body=$(watch_body "$main" "$pr_state" "$pr_head")
	if [ -z "$existing" ]; then
		gh issue create --title "$watch_title" --body "$body"
		return 0
	fi
	local current
	current=$(gh issue view "$existing" --json body --jq .body)
	if [ "$current" = "$body" ]; then
		echo "meowcaller: issue #$existing already says this"
		return 0
	fi
	gh issue edit "$existing" --body "$body"
	gh issue comment "$existing" --body "O upstream andou de novo desde a última atualização deste aviso. Fixado \`$COMMIT\`, \`main\` em \`$main\`, PR #$PR \`$pr_state\` em \`$pr_head\`."
}

case "${1:-}" in
sync) cmd_sync ;;
check) cmd_check ;;
watch) cmd_watch ;;
*)
	echo "usage: $0 sync|check|watch" >&2
	exit 2
	;;
esac
