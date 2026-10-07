# third_party

`meowcaller/` is [purpshell/meowcaller](https://github.com/purpshell/meowcaller) at the commit in `meowcaller.pin`, with these changes on top, and `go.mod` replaces the module with it:

- the import path `github.com/polymorfa/hypermeow` (the whatsmeow fork upstream builds against) rewritten to `go.mau.fi/whatsmeow`, which `meowcaller.sh` does as a plain text substitution;
- the patches in `patches/meowcaller/`, applied in lexical order. Each one says where it came from and why it is here.
- upstream's tests left out: every `*_test.go` and every `testdata/` directory. Nothing here runs their suite, and its crypto known-answer vectors read as secrets to the push guard of this public repository. Everything that builds is kept.

The copy is derived, never edited by hand. `make meowcaller-check` (part of `make check`) rebuilds it from upstream in a scratch directory and fails when the committed tree differs, so a change to the copy goes in a patch and `make meowcaller` regenerates the tree.

To bump: change `COMMIT` in `meowcaller.pin`, run `make meowcaller`, and fix whatever patch no longer applies. A patch that stops applying means upstream changed the code it touches: read the issue the patch names before rewriting it. When the pull request named by `PR` is merged upstream and the pin moves past it, its patch goes away and `PR`/`PR_HEAD` follow the next one we carry, if any.

The weekly workflow `meowcaller-upstream` runs `meowcaller.sh watch`: it compares the pin with upstream `main` and `PR_HEAD` with that pull request, and opens (or updates) an issue titled "o meowcaller andou" when either moved. That issue is the signal to bump.
