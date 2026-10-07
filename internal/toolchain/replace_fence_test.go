package toolchain

import (
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// A module replaced by a directory in this repository has to be in the image before
// `go mod download` runs, or the build step that reads go.mod fails on a path that is not
// there. Nothing fails today while no package imports the replaced module: download skips
// what nobody requires. It fails the day somebody imports it, in the image job only, which
// is the one a developer does not run, so the rule is held here instead of discovered there.
func TestEveryLocalReplaceIsCopiedBeforeTheModuleDownload(t *testing.T) {
	out, err := exec.CommandContext(t.Context(), "go", "mod", "edit", "-json", "../../go.mod").Output()
	if err != nil {
		t.Fatalf("go mod edit -json: %v", err)
	}
	var mod struct {
		Replace []struct {
			Old struct{ Path string }
			New struct{ Path, Version string }
		}
	}
	if err := json.Unmarshal(out, &mod); err != nil {
		t.Fatalf("read go mod edit -json: %v", err)
	}

	raw, err := os.ReadFile("../../Dockerfile")
	if err != nil {
		t.Fatalf("read Dockerfile: %v", err)
	}
	copied, download := copiedBeforeDownload(string(raw))
	if !download {
		t.Fatal("no `RUN go mod download` in the Dockerfile: the build moved, or this reads nothing")
	}

	local := 0
	for _, r := range mod.Replace {
		if r.New.Version != "" || !strings.HasPrefix(r.New.Path, "./") {
			continue // a module version, fetched like any other
		}
		local++
		dir := strings.TrimSuffix(strings.TrimPrefix(r.New.Path, "./"), "/")
		if !copiedCovers(copied, dir) {
			t.Errorf("go.mod replaces %s with %s, and the Dockerfile does not COPY it before `go mod download`", r.Old.Path, r.New.Path)
		}
	}
	if local == 0 {
		t.Fatal("go.mod has no replace by a local directory: third_party/meowcaller left, or this reads nothing")
	}
}

// copiedBeforeDownload returns the sources of every COPY above the first module download,
// and whether there is one.
func copiedBeforeDownload(dockerfile string) ([]string, bool) {
	var copied []string
	for _, line := range strings.Split(dockerfile, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		switch strings.ToUpper(fields[0]) {
		case "RUN":
			if strings.Contains(line, "go mod download") {
				return copied, true
			}
		case "COPY":
			args := fields[1:]
			for len(args) > 0 && strings.HasPrefix(args[0], "--") {
				args = args[1:]
			}
			if len(args) > 1 {
				copied = append(copied, args[:len(args)-1]...)
			}
		}
	}
	return copied, false
}

func copiedCovers(copied []string, dir string) bool {
	for _, src := range copied {
		src = strings.TrimSuffix(strings.TrimPrefix(src, "./"), "/")
		if src == "." || src == dir || strings.HasPrefix(dir, src+"/") {
			return true
		}
	}
	return false
}
