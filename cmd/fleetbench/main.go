// Command fleetbench runs a real fleet of connector processes against a real PostgreSQL
// and a real Redis, applies load, forces ownership to move, and asserts the operational
// invariants that only two processes under load can disprove.
//
// # What it runs, and what that leaves unmeasured
//
// It runs `WAC_ENGINE=fake`, by decision, and the decision is in issue #264: leases,
// epochs, shards, `seq`, fencing, the store and the transport are all agnostic to the
// engine, and they are the half with no measurement. The whatsmeow engine under an
// ownership change is NOT covered here and cannot be: pairing a real account needs a
// physical device, which is the `NEEDS_PHYSICAL_DEVICE` case, and no run of this bench
// ever touches a real WhatsApp account.
//
// So what stays unmeasured after a green run is named rather than left implicit: whether
// a real socket, a real pairing and a real message survive an ownership change. What a
// green run does say is that the fleet's own machinery -- one owner at a time, an epoch
// that rises on every change, `seq` monotonic per `(sid, epoch)`, a session that stays on
// one shard, a command that is not carried out twice -- holds across processes, under
// load, on real servers.
//
// # Why it is not in `make check`
//
// It builds a binary, starts processes and waits on real clocks, which is minutes rather
// than seconds. `make check` runs on every change and this does not belong there. The
// exemption is recorded in `internal/toolchain`, where the suite reads it, rather than in
// a comment nobody's tests open.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func main() {
	sessions := flag.Int("sessions", 40, "how many sessions the fleet carries")
	shards := flag.Int("shards", 4, "how many event shards the fleet publishes to (more than one, or the shard half of invariant 3 is invisible)")
	processes := flag.Int("processes", 2, "how many connector processes to run (two or more, or there is no ownership to move)")
	sends := flag.Int("sends", 6, "how many commands to put in flight per session during the handover")
	maxAdoption := flag.Duration("max-adoption", 0, "optional: a mass adoption slower than this is reported as a measurement outside its range, never as a defect")
	keep := flag.Bool("keep", false, "keep the run's database, keys and logs instead of giving them back")
	flag.Parse()

	// An error back from `runBench` means it stopped before there was a report to write,
	// so this is the only place that prints one. Once there is a report, the outcome and
	// its reason are printed with it, and `runBench` answers with the code alone.
	code, err := runBench(*sessions, *shards, *processes, *sends, *maxAdoption, *keep)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n=== %s (exit %d) ===\n%v\n", outcomeSetup.label(), int(outcomeSetup), err)
		os.Exit(int(outcomeSetup))
	}
	os.Exit(int(code))
}

func runBench(sessions, shards, processes, sends int, maxAdoption time.Duration, keep bool) (outcome, error) {
	if processes < 2 {
		return outcomeSetup, fmt.Errorf("%w: -processes is %d, and an ownership change needs at least two", errSetup, processes)
	}
	if shards < 2 {
		return outcomeSetup, fmt.Errorf("%w: -shards is %d; with one shard a session cannot be seen on two, "+
			"so the shard half of invariant 3 would pass by construction", errSetup, shards)
	}

	// Its own context, cancelled on the first interrupt, so that a run somebody stops by
	// hand still gives back its database, its keys and its processes.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	s, err := preflight(ctx)
	if err != nil {
		return outcomeSetup, err
	}

	current, err := os.Getwd()
	if err != nil {
		return outcomeSetup, fmt.Errorf("%w: find the module to build from: %w", errSetup, err)
	}
	moduleDir, err := moduleRoot(current)
	if err != nil {
		return outcomeSetup, err
	}

	active, err := newRun(ctx, s)
	if err != nil {
		return outcomeSetup, fmt.Errorf("%w: %w", errSetup, err)
	}

	// Before anything of this run exists, so that what it writes can be told from what
	// was already on a Redis it shares with everything else on this machine.
	before, err := active.snapshotKeys(ctx)
	if err != nil {
		return outcomeSetup, fmt.Errorf("%w: read the Redis this run is about to share: %w", errSetup, err)
	}

	workDir, err := os.MkdirTemp("", "wac-fleetbench-"+active.id+"-")
	if err != nil {
		return outcomeSetup, fmt.Errorf("%w: make a directory for this run: %w", errSetup, err)
	}

	rep := &report{engine: "fake"}
	rep.note("motor: fake, por decisao (issue #264). Nenhuma corrida desta bancada toca conta real do WhatsApp.")
	rep.note("nao coberto: o motor whatsmeow sob troca de dono. Parear conta de verdade exige aparelho fisico " +
		"(NEEDS_PHYSICAL_DEVICE), entao o que um verde daqui NAO diz e se um socket, um pareamento e uma " +
		"mensagem de verdade sobrevivem a troca de dono.")
	rep.note("banco desta corrida: " + active.database + " · prefixo: " + active.prefix + " · arquivos: " + workDir)

	binary, sum, err := buildConnector(ctx, moduleDir, workDir)
	if err != nil {
		_ = active.cleanup(context.WithoutCancel(ctx))
		return outcomeSetup, fmt.Errorf("%w: %w", errSetup, err)
	}
	rep.binary, rep.binarySum = binary, sum

	group := &fleet{binary: binary, binarySum: sum, dir: workDir, env: active.connectorEnv(shards)}
	defer func() {
		group.shutdown()
		if keep {
			rep.note("guardado a pedido (-keep): banco " + active.database + ", prefixo " + active.prefix + ", " + workDir)
			return
		}
		for _, trouble := range active.cleanup(context.WithoutCancel(ctx)) {
			fmt.Fprintf(os.Stderr, "limpeza: %s\n", trouble)
		}
		_ = os.RemoveAll(workDir)
	}()

	if err := measure(ctx, active, group, rep, benchPlan{
		sessions: sessions, shards: shards, processes: processes, sends: sends, maxAdoption: maxAdoption,
	}); err != nil {
		// The report still goes out, because what it holds up to the point the run
		// stopped is the only evidence of where it stopped, and it carries the run's own
		// database and prefix for anyone who wants to look.
		//
		// Under the outcome the error says, and not under `outcomeSetup` for everything
		// that stopped early: a run that gave up because it had already found a broken
		// lease is a defect, and filing it as "the machine was not ready" is how the
		// clearest red in the whole bench ends up in the category a reader skips.
		stopped := outcomeSetup
		if errors.Is(err, errInvariantBroken) {
			stopped = rep.outcome()
		}
		rep.write(os.Stdout, stopped, err)
		return stopped, nil
	}

	// The bench's own hygiene, checked rather than promised: a run that wrote outside its
	// prefix touched a fleet it does not own, and nothing it asserted about "the" fleet
	// can be trusted afterwards. Reported as setup and not as a broken invariant, because
	// what is wrong is the instrument.
	strayed, err := active.unprefixed(context.WithoutCancel(ctx), before)
	if err != nil {
		rep.note("nao deu para conferir se a corrida escreveu fora do proprio prefixo: " + err.Error())
	} else if len(strayed) > 0 {
		shown := strayed
		if len(shown) > 10 {
			shown = shown[:10]
		}
		reason := fmt.Errorf("%w: a corrida escreveu %d chaves fora do prefixo %s, entao ela tocou uma "+
			"frota que nao e a dela: %v", errSetup, len(strayed), active.prefix, shown)
		rep.write(os.Stdout, outcomeSetup, reason)
		return outcomeSetup, nil
	} else {
		rep.note("nenhuma chave fora do prefixo " + active.prefix + " apareceu no Redis durante a corrida")
	}

	final := rep.outcome()
	rep.write(os.Stdout, final, nil)
	return final, nil
}

type benchPlan struct {
	sessions    int
	shards      int
	processes   int
	sends       int
	maxAdoption time.Duration
}

// moduleRoot walks up to the directory holding go.mod, which is the tree whose connector
// this run has to build. Walking up rather than trusting the working directory is what
// lets a mutation run the bench from a copy of the tree and still build that copy.
func moduleRoot(from string) (string, error) {
	dir := from
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("%w: no go.mod at or above %s, so there is no tree to build the connector from", errSetup, from)
		}
		dir = parent
	}
}
