package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// The pool reading has to count the FLEET's connections, and the two halves that decide
// that live in different files.
//
// `runURLs` puts a name of its own on the connector's URL, and the query counts rows
// carrying that name. Read the other way -- everything that is NOT the bench's name, which
// is how it was written -- the number depends on a string nobody in the run controls: a
// `WAC_TEST_DATABASE_URL` carrying `application_name=fleetbench`, or a `PGAPPNAME` with
// that value, left every connector connection excluded, the reading at zero and the
// ceiling check passing over a fleet it had not counted.
//
// Executed and not read as source, because what is wrong in that version is one operator
// inside a string, and a check that greps for the operator is a check that a rewording
// defeats. `pg_stat_activity` is PostgreSQL's, so without a server this says what it did
// not measure rather than passing.
func TestThePoolReadingCountsTheFleetAndNotTheBench(t *testing.T) {
	t.Parallel()

	server := os.Getenv(databaseVar)
	if server == "" {
		t.Skipf("%s nao nomeia servidor: a leitura da pool le pg_stat_activity, que e do "+
			"PostgreSQL, entao esta passagem nao foi medida nesta corrida", databaseVar)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	admin, err := sql.Open("postgres", server)
	if err != nil {
		t.Fatalf("abrir %s: %v", databaseVar, err)
	}
	// Closed last of all, because the drop below runs through it. Cleanups are LIFO, so the
	// order registered here is the reverse of the order they run in: this one, then the
	// drop, then the connections the reading is taken over.
	t.Cleanup(func() { _ = admin.Close() })

	name := fmt.Sprintf("wacpool%d", time.Now().UnixNano())
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE "`+name+`"`); err != nil {
		t.Fatalf("criar o banco desta medicao: %v", err)
	}
	// Registered BEFORE the connections below, so it runs after them: cleanups are LIFO,
	// and a `DROP DATABASE` while this test's own connections are still open is refused.
	t.Cleanup(func() {
		if _, err := admin.ExecContext(context.WithoutCancel(ctx), `DROP DATABASE "`+name+`"`); err != nil {
			t.Errorf("o banco %s desta medicao ficou no servidor: %v", name, err)
		}
	})

	// The supplied URL already carries the bench's own `application_name`, which is the
	// collision the reading has to survive: read by exclusion, every connector connection
	// would be excluded and the count would come back zero over a fleet it had not counted.
	fleetURL, benchURL, err := runURLs(server+suppliedName(server), name)
	if err != nil {
		t.Fatalf("derivar as URLs: %v", err)
	}

	// One connection under each name, both held open while the reading is taken. A
	// connection that the pool has not opened yet is not in `pg_stat_activity`, so each is
	// used once before it counts as held.
	held := func(url, as string) *sql.DB {
		t.Helper()
		db, err := sql.Open("postgres", url)
		if err != nil {
			t.Fatalf("abrir a conexao de %s: %v", as, err)
		}
		t.Cleanup(func() { _ = db.Close() })
		db.SetMaxIdleConns(1)
		var one int
		if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
			t.Fatalf("usar a conexao de %s: %v", as, err)
		}
		return db
	}
	held(fleetURL, "frota")
	held(benchURL, "bancada")

	count, err := poolBackends(ctx, &run{database: name, benchURL: benchURL})
	if err != nil {
		t.Fatalf("ler a pool: %v", err)
	}
	// The bench's own reader opens a connection of its own to take the reading, so the
	// wrong direction counts two and not one: this asserts the number, not merely that it
	// is non-zero.
	if count != 1 {
		t.Errorf("a leitura contou %d conexoes com uma da frota e duas da bancada abertas, e a URL "+
			"dada ja trazia application_name=%s: ou ela conta por exclusao do nome da bancada, ou o "+
			"nome do chamador chegou na frota", count, benchApplicationName)
	}
}

// suppliedName is the query fragment that puts the bench's own application_name on the URL
// the caller supplies, appended the way a caller would have written it.
func suppliedName(server string) string {
	if strings.Contains(server, "?") {
		return "&application_name=" + benchApplicationName
	}
	return "?application_name=" + benchApplicationName
}

// And the collision the finding named: a caller whose URL already carries the bench's own
// application_name must not hand that name to the fleet.
func TestASuppliedApplicationNameNeverReachesTheFleet(t *testing.T) {
	t.Parallel()

	fleetURL, _, err := runURLs("postgres://u:p@h:5432/base?application_name="+benchApplicationName, "wacbench1")
	if err != nil {
		t.Fatalf("runURLs recusou uma URL valida: %v", err)
	}
	if strings.Contains(fleetURL, "application_name="+benchApplicationName+"&") ||
		strings.HasSuffix(fleetURL, "application_name="+benchApplicationName) {
		t.Errorf("a frota ficou com o application_name da bancada, entao a leitura da pool nao "+
			"separa as duas:\n%s", fleetURL)
	}
}
