package redisx_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The third time the same false claim was found, and the first two were found only because
// somebody happened to be working next to them.
//
// `handoff:<sid>` had a key constructor until 2f1305e (#192) deleted it, and nothing was
// ever written behind it. Three sentences went on describing it as though it were there:
// contract/PROTOCOL.md (#244, corrected by #253), and two Go comments in internal/session
// (#252). The prose fences this repository already has read the contract; no fence reads a
// Go comment, so those two stayed green through every suite, every review and two rounds
// that were about this exact family.
//
// Matched by claim rather than by file. A fence that knew the two files #252 names would
// go green on the fourth occurrence, which is the one nobody has found yet. The shape of a
// claim is the word next to a key noun: the ownership handoff between instances is real,
// is discussed in about thirty comments across internal/engine, internal/store and
// manager.go itself, and none of those calls it a key.
//
// Most of what separates the two is the comment block, not the sixty-character window:
// with no window at all this tree still reports nothing outside the two exemptions, because
// a sentence about the ownership handoff and a sentence about a key almost never share a
// block. The window is the margin on top of that, and it is measured rather than guessed --
// the nearest legitimate mention with a key noun in the same block is the one in
// internal/store/availability.go, 328 characters away, so widening this past 328 is where
// the fence would start reporting correct prose, which is how a fence gets deleted.
//
// The exemptions are the sentences whose job is to say the key does not exist. They are
// listed with their reason rather than skipped by pattern, so adding one is a decision
// somebody writes down.
var handoffProseThatMayNameTheKey = map[string]string{
	"contract/PROTOCOL.md": "the contract's own paragraph, which says the key was declared once and removed. " +
		"Fenced word for word by TestTheContractKeepsHandoffAndHandbackApart above, so a wrong claim here turns that red instead.",
	"internal/redisx/handoff_prose_test.go": "this file. It has to name the claim in order to describe it, and the sweep below would " +
		"otherwise report the sentence explaining the sweep.",
	"internal/redisx/keys_fence_test.go": "the #248 fence next door, whose comments name the key to say it never had anything behind it. " +
		"The contract prose it guards is fenced word for word by TestTheContractKeepsHandoffAndHandbackApart there.",
}

func TestNoCommentClaimsAHandoffKeyExists(t *testing.T) {
	t.Parallel()

	root := filepath.Join("..", "..")
	claim := regexp.MustCompile(`(?is)handoff.{0,60}?\b(keys?|key set|constructors?)\b|\b(keys?|key set|constructors?)\b.{0,60}?handoff|handoff:\S|\bHandoff\(`)

	var found int
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" || d.Name() == "node_modules" {
				return fs.SkipDir
			}
			return nil
		}
		rel := filepath.ToSlash(strings.TrimPrefix(path, root+string(filepath.Separator)))
		var prose []string
		switch filepath.Ext(path) {
		case ".go":
			prose = goComments(t, path)
		case ".md":
			body, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			prose = []string{string(body)}
		default:
			return nil
		}
		for _, text := range prose {
			flat := strings.Join(strings.Fields(text), " ")
			hit := claim.FindString(flat)
			if hit == "" {
				continue
			}
			found++
			if _, exempt := handoffProseThatMayNameTheKey[rel]; exempt {
				continue
			}
			t.Errorf("%s calls handoff a key: %q\n"+
				"There is no such key and no constructor for one: 2f1305e (#192) deleted it and nothing was ever written behind it. "+
				"What is true is that no command in the protocol asks an owner to give a session up on demand, and that `wa:handback:<sid>`, "+
				"which does exist, is an owner deciding to give one up itself rather than a way to ask. Say that instead, or add this file to "+
				"handoffProseThatMayNameTheKey with the reason it has to name the key.", rel, hit)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk the tree: %v", err)
	}
	if found < len(handoffProseThatMayNameTheKey) {
		t.Errorf("the sweep matched %d claim(s) but %d file(s) are exempted: the fence is not reading what it thinks it is, "+
			"and would pass on a tree where the claim came back", found, len(handoffProseThatMayNameTheKey))
	}
}

// goComments returns a file's comments, code excluded. Identifiers are not prose: this
// package's own `handoffWait` and `perishableHandoff` name a timer in the whatsmeow engine
// and say nothing about a key, and a fence that read them would be reporting code for
// being named after the thing it implements.
func goComments(t *testing.T, path string) []string {
	t.Helper()

	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ParseComments|parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	var out []string
	for _, group := range file.Comments {
		out = append(out, group.Text())
	}
	return out
}
