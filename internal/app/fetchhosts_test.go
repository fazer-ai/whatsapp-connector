package app_test

import (
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/app"
)

// The list is read at startup, and one that is not a list of hosts stops the instance
// rather than starting it with a list that refuses every file or allows the wrong ones
// (#31). Unset, it is empty: every host, as before.
func TestTheFetchHostListIsReadAtStartup(t *testing.T) {
	t.Setenv("WAC_INSTANCE", "inst-a")

	cfg, err := app.LoadConfig("host")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if len(cfg.MediaFetchHosts) != 0 {
		t.Fatalf("with nothing set the list is %v, want empty", cfg.MediaFetchHosts)
	}

	t.Setenv("WAC_MEDIA_FETCH_HOSTS", "rails:3000, blobs.example.com")
	if cfg, err = app.LoadConfig("host"); err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if len(cfg.MediaFetchHosts) != 2 {
		t.Fatalf("the list read as %v, want two hosts", cfg.MediaFetchHosts)
	}

	for _, bad := range []string{"http://rails:3000", "rails:abc"} {
		t.Setenv("WAC_MEDIA_FETCH_HOSTS", bad)
		if _, err := app.LoadConfig("host"); err == nil {
			t.Fatalf("WAC_MEDIA_FETCH_HOSTS=%q was accepted", bad)
		}
	}
}
