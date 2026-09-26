package app

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/observability"
)

// contract/PROTOCOL.md tells a client how long a stranded group creation lasts, in hours,
// and a client plans its escalation on that number (#277). It is this constant's, and a
// number written in prose does not move when the constant does.
func TestTheContractNamesTheGroupCreateCeiling(t *testing.T) {
	t.Parallel()

	source, err := os.ReadFile(filepath.Join("..", "..", "contract", "PROTOCOL.md"))
	if err != nil {
		t.Fatalf("read the contract: %v", err)
	}
	var paragraph string
	for _, block := range strings.Split(string(source), "\n\n") {
		if strings.HasPrefix(block, "**`not_settled` is not a promise") {
			paragraph = strings.Join(strings.Fields(block), " ")
		}
	}
	if paragraph == "" {
		t.Fatal("contract/PROTOCOL.md has no `not_settled` paragraph to read the ceiling from")
	}
	if groupCreateCeiling%time.Hour != 0 {
		t.Fatalf("groupCreateCeiling is %v, which the contract cannot state in whole hours", groupCreateCeiling)
	}
	want := strconv.Itoa(int(groupCreateCeiling/time.Hour)) + " hours after its first delivery"
	if !strings.Contains(paragraph, want) {
		t.Fatalf("the `not_settled` paragraph does not say %q, which is what groupCreateCeiling is", want)
	}
}

// The connector's own sweep passes the ceiling, and not only the store's statement knows it
// (#277): a stranded intent asked about a moment ago, begun longer than the ceiling ago, is
// gone after one pass, while an unnamed one inside it and a named one as old stay.
func TestTheSweepPassReachesAStrandedGroupCreation(t *testing.T) {
	t.Parallel()

	container := openTestStore(t)
	scoped := container.For("sid-1")
	now := time.Now()
	for attempt, began := range map[string]time.Time{
		"idem:stranded": now.Add(-groupCreateCeiling - time.Hour),
		"idem:inside":   now.Add(-groupCreateCeiling + time.Hour),
		"idem:named":    now.Add(-groupCreateCeiling - time.Hour),
	} {
		if _, _, err := scoped.BeginGroupCreate(t.Context(), attempt, "WAC"+attempt, "Obras", began); err != nil {
			t.Fatalf("begin %s: %v", attempt, err)
		}
		if _, _, err := scoped.BeginGroupCreate(t.Context(), attempt, "WACAGAIN", "Obras", now); err != nil {
			t.Fatalf("retry %s: %v", attempt, err)
		}
	}
	if err := scoped.FinishGroupCreate(t.Context(), "idem:named", "120363041234567890@g.us"); err != nil {
		t.Fatalf("name the group: %v", err)
	}

	connector := &Connector{
		metrics: observability.New(),
		cfg:     Config{MediaRefetch: DefaultMediaRefetch},
		log:     zerolog.Nop(),
		store:   container,
	}
	if stop := connector.sweepPartsOnce(t.Context()); stop {
		t.Fatal("the sweep pass stopped as if its context were done")
	}

	for attempt, want := range map[string]bool{"idem:stranded": false, "idem:inside": true, "idem:named": true} {
		if _, found, err := scoped.GroupCreation(t.Context(), attempt); err != nil {
			t.Fatalf("read %s: %v", attempt, err)
		} else if found != want {
			t.Fatalf("%s present=%v after a sweep pass, want %v", attempt, found, want)
		}
	}
}
