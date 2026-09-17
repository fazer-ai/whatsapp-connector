package app

import (
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/fazer-ai/whatsapp-connector/internal/observability"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
)

// Every value the source label can take has to be in the list eviction walks.
//
// This is a fence over a mistake already made once in this file. The eviction used to
// spell `sourceRead` and `sourceClaim` by hand in two `DeleteLabelValues` calls, and the
// round that added `sourceRestored` left them as they were: the new series would have
// been counted, never dropped, and kept for the life of the process. Nothing about the
// exposition says that, because a series that is never evicted looks exactly like one
// that is still being counted against.
//
// Reading the constants rather than a list written next to them, because a list is the
// thing that drifts. A constant named `sourceSomething` is a value of that label by
// construction, and one missing from `everySource` is the bug above.
func TestEverySourceValueIsOneEvictionWalks(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "run.go", nil, 0)
	if err != nil {
		t.Fatalf("parse run.go: %v", err)
	}

	var declared, listed []string
	ast.Inspect(file, func(node ast.Node) bool {
		decl, ok := node.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for _, name := range decl.Names {
			if strings.HasPrefix(name.Name, "source") && name.Name != "source" {
				declared = append(declared, name.Name)
			}
			if name.Name != "everySource" {
				continue
			}
			for _, value := range decl.Values {
				composite, ok := value.(*ast.CompositeLit)
				if !ok {
					continue
				}
				for _, element := range composite.Elts {
					if ident, ok := element.(*ast.Ident); ok {
						listed = append(listed, ident.Name)
					}
				}
			}
		}
		return true
	})

	if len(declared) < 2 {
		t.Fatalf("found %d source constants in run.go (%v): the parse is not reading the file "+
			"this fence is about, and a parse that finds nothing passes it without looking",
			len(declared), declared)
	}
	if len(listed) == 0 {
		t.Fatal("found no elements in everySource: eviction would walk nothing and every " +
			"series would be kept for the life of the process")
	}
	for _, name := range declared {
		if !slices.Contains(listed, name) {
			t.Errorf("%s is a value of the source label and is not in everySource, so a series "+
				"carrying it is counted and never dropped. Add it to the list.", name)
		}
	}
	for _, name := range listed {
		if !slices.Contains(declared, name) {
			t.Errorf("everySource names %s, which is not a source constant in run.go", name)
		}
	}
}

// And the behaviour the fence stands in front of: a series under the new source is
// dropped like any other when its label goes quiet.
func TestARestoredSeriesIsDroppedWhenItsSessionGoesQuiet(t *testing.T) {
	t.Parallel()

	metrics := observability.New()
	c := &Connector{metrics: metrics, cfg: Config{Instance: "inst-alone"}}

	c.measure([]transport.Delivery{{
		Command:         protocol.Command{SID: "wac224-restored", Type: protocol.CommandSessionWake},
		DeliveredBefore: true,
		TakenFrom:       "inst-alone",
		Deliveries:      4,
	}})
	if got := testutil.CollectAndCount(metrics.CommandsDeliveredAgain, "wac_commands_delivered_again_total"); got != 1 {
		t.Fatalf("after one restored delivery the exposition has %d series, want 1", got)
	}

	c.forgetLabelsGoneQuiet(time.Now().Add(labelQuiet + time.Second))

	if got := testutil.CollectAndCount(metrics.CommandsDeliveredAgain, "wac_commands_delivered_again_total"); got != 0 {
		t.Errorf("the session went quiet and its restored series is still there (%d left): "+
			"eviction spells the sources by hand and this one was left out", got)
	}
}
