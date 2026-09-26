package toolchain_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// The variable a test reads to run against a real Redis, spelled in two pieces so this
// file, which has to name it, does not count as one of the files it looks for.
const realRedisVar = "WAC_TEST_" + "REDIS_URL"

// Every package with a test that runs against a real Redis is in `make test-redis`, which
// is the only pass that sets the variable: `make test` clears it, and CI runs that target
// against the newest Redis and against the oldest one README.md supports.
//
// A test of that kind in a package the target does not name is skipped in every pass
// there is, and green by skipping. #279 is the shape of what that hides: a command form
// the floor refuses, in `internal/app`, which the target did not name, so the one pass that
// would have caught it did so by not running.
func TestEveryPackageThatTestsAgainstARealRedisIsInTheRedisPass(t *testing.T) {
	t.Parallel()

	_, recipe := recipeOf(t, "test-redis")
	var named []string
	for _, field := range strings.Fields(recipe) {
		if strings.HasPrefix(field, "./") {
			named = append(named, filepath.Clean(field))
		}
	}
	if len(named) == 0 {
		t.Fatalf("the test-redis recipe names no package; it was rewritten and this check reads nothing:\n%s", recipe)
	}

	root := filepath.Join("..", "..")
	var missing []string
	found := 0
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if name := entry.Name(); name == ".git" || name == "vendor" || name == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !strings.Contains(string(source), realRedisVar) {
			return nil
		}
		found++
		dir, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		if !slices.Contains(named, dir) && !slices.Contains(missing, dir) {
			missing = append(missing, dir)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk the repository: %v", err)
	}
	if found == 0 {
		t.Fatalf("no test reads %s, so this check has nothing to hold; it lost its way to them", realRedisVar)
	}
	if len(missing) > 0 {
		t.Fatalf("these packages have tests that run against a real Redis, and `make test-redis` does not name them, "+
			"so every pass skips them: %v. Add them to the test-redis recipe in the Makefile (it names %v)", missing, named)
	}
}
