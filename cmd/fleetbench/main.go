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
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func main() {
	sessions := flag.Int("sessions", 40, "how many sessions the fleet carries")
	shards := flag.Int("shards", 4, "how many event shards the fleet publishes to (more than one, or the shard half of invariant 3 is invisible)")
	processes := flag.Int("processes", 3, "how many connector processes to run. Two moves ownership; three is what "+
		"reaches invariant 1's fence, because freezing an owner needs a second peer left to take its sessions")
	sends := flag.Int("sends", 6, "how many commands to put in flight per session during the handover")
	maxAdoption := flag.Duration("max-adoption", 0, "optional: a mass adoption slower than this is reported as a measurement outside its range, never as a defect")
	keep := flag.Bool("keep", false, "keep the run's database, keys and logs instead of giving them back")
	flag.Parse()

	// An error back from `runBench` means it stopped before there was a report to write,
	// so this is the only place that prints one. Once there is a report, the outcome and
	// its reason are printed with it, and `runBench` answers with the code alone.
	code, err := runBench(context.Background(), benchIO{out: os.Stdout, errOut: os.Stderr},
		*sessions, *shards, *processes, *sends, *maxAdoption, *keep)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n=== %s (exit %d) ===\n%v\n", outcomeSetup.label(), int(outcomeSetup), err)
		os.Exit(int(outcomeSetup))
	}
	os.Exit(int(code))
}

// benchIO is where a run writes, and the one seam a test has into its middle.
type benchIO struct {
	out, errOut io.Writer
	// afterBuild runs once the connector under test is built, before the fleet starts. Nil
	// in production. A test cancels the run's context here to reach a printed report in
	// seconds rather than after a whole run: what it asks is what the report says, and the
	// fleet is not part of the answer.
	afterBuild func()
}

func runBench(parent context.Context, dest benchIO, sessions, shards, processes, sends int,
	maxAdoption time.Duration, keep bool,
) (outcome, error) {
	if processes < 2 {
		return outcomeSetup, fmt.Errorf("%w: -processes is %d, and an ownership change needs at least two", errSetup, processes)
	}
	if shards < 2 {
		return outcomeSetup, fmt.Errorf("%w: -shards is %d; with one shard a session cannot be seen on two, "+
			"so the shard half of invariant 3 would pass by construction", errSetup, shards)
	}
	// The two sizes the run allocates from, checked beside the two it already checked.
	// A negative one panics inside `make`, and it panics LATE: the connect phase and the
	// mass adoption finish first, so the run has already created a database and three
	// processes and given back neither. A setup error is what a bad flag is.
	if sessions < 1 {
		return outcomeSetup, fmt.Errorf("%w: -sessions is %d, and a fleet carrying no session "+
			"measures nothing", errSetup, sessions)
	}
	if sends < 1 {
		return outcomeSetup, fmt.Errorf("%w: -sends is %d, and the handover needs a command in "+
			"flight to have anything to redeliver", errSetup, sends)
	}

	// Its own context, cancelled on the first interrupt, so that a run somebody stops by
	// hand still gives back its database, its keys and its processes.
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
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

	rep := &report{engine: "fake"}
	rep.note("motor: fake, por decisao (issue #264). Nenhuma corrida desta bancada toca conta real do WhatsApp.")
	rep.note("nao coberto: o motor whatsmeow sob troca de dono. Parear conta de verdade exige aparelho fisico " +
		"(NEEDS_PHYSICAL_DEVICE), entao o que um verde daqui NAO diz e se um socket, um pareamento e uma " +
		"mensagem de verdade sobrevivem a troca de dono.")

	// Armed here and not further down, because from the line above this one there is
	// already a database on the server with this run's name on it. Every failure between
	// that line and the fleet starting -- a snapshot that cannot read, an interrupt, a
	// directory that will not be made -- returned without dropping it, and what that
	// leaves behind is a database nobody will ever look for again.
	//
	// `workDir` is read at call time, so the deferred call sees the directory whenever it
	// came to exist rather than the empty string it holds here.
	workDir := ""
	group := &fleet{}
	defer func() {
		group.shutdown()
		if keep {
			// The report already says so when it went out. A run that stopped before
			// printing one -- a connector that would not build, an interrupt before the
			// directory existed -- has nowhere else to say what it left behind.
			if !rep.written {
				_, _ = fmt.Fprintln(dest.errOut, keptNote(active, workDir))
			}
			return
		}
		for _, trouble := range active.cleanup(context.WithoutCancel(ctx)) {
			_, _ = fmt.Fprintf(dest.errOut, "limpeza: %s\n", trouble)
		}
		if workDir != "" {
			_ = os.RemoveAll(workDir)
		}
	}()

	// Before anything of this run exists, so that what it writes can be told from what
	// was already on a Redis it shares with everything else on this machine.
	before, err := active.snapshotKeys(ctx)
	if err != nil {
		return outcomeSetup, fmt.Errorf("%w: read the Redis this run is about to share: %w", errSetup, err)
	}

	workDir, err = os.MkdirTemp("", "wac-fleetbench-"+active.id+"-")
	if err != nil {
		return outcomeSetup, fmt.Errorf("%w: make a directory for this run: %w", errSetup, err)
	}

	rep.note("banco desta corrida: " + active.database + " · prefixo: " + active.prefix + " · arquivos: " + workDir)
	// Here, before every path that prints the report, and not in the deferred cleanup: a
	// note added there lands in a report that has already been printed and is never
	// printed again, so whoever asked for -keep was not told what to clean up (#310).
	// TestTheKeepNoteIsTakenBeforeTheReportIsPrinted holds the order.
	if keep {
		rep.note(keptNote(active, workDir))
	}

	binary, sum, err := buildConnector(ctx, moduleDir, workDir)
	if err != nil {
		return outcomeSetup, fmt.Errorf("%w: %w", errSetup, err)
	}
	rep.binary, rep.binarySum = binary, sum
	*group = fleet{binary: binary, binarySum: sum, dir: workDir, env: active.connectorEnv(shards)}
	if dest.afterBuild != nil {
		dest.afterBuild()
	}

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
		stopped := stoppedOutcome(rep, err)
		rep.write(dest.out, stopped, err)
		return stopped, nil
	}

	// The bench's own hygiene, checked rather than promised: a run that wrote outside its
	// prefix touched a fleet it does not own, and nothing it asserted about "the" fleet
	// can be trusted afterwards. Reported as setup and not as a broken invariant, because
	// what is wrong is the instrument.
	// Two questions, and only one of them a before/after inventory can answer.
	//
	// "Did a key outside this run's prefix appear" it can answer. "Did this run write it"
	// it cannot: this Redis is shared on purpose, and another fleet, another bench or a
	// developer's own client can write during the same minutes. Failing the run on that
	// would reject a correctly isolated run because somebody else was working.
	//
	// So the attributable half is the one that fails: a key carrying this run's id was
	// written by this run, wherever it landed. Everything else is reported by name and
	// left to whoever reads it, which is more than a silent pass and less than a verdict
	// the evidence does not support.
	strayed, err := active.unprefixed(context.WithoutCancel(ctx), before)
	switch {
	case err != nil:
		rep.note("nao deu para conferir se a corrida escreveu fora do proprio prefixo: " + err.Error())
	case len(strayed) == 0:
		rep.note("nenhuma chave nova fora do prefixo " + active.prefix + " apareceu no Redis durante a corrida")
	default:
		mine := []string{}
		for _, key := range strayed {
			if strings.Contains(key, active.id) {
				mine = append(mine, key)
			}
		}
		if len(mine) > 0 {
			reason := fmt.Errorf("%w: a corrida escreveu %d chaves fora do prefixo %s e elas carregam o id "+
				"desta corrida, entao sao dela: %s", errSetup, len(mine), active.prefix, shortList(mine))
			rep.write(dest.out, outcomeSetup, reason)
			return outcomeSetup, nil
		}
		rep.note(fmt.Sprintf("apareceram %d chaves novas fora do prefixo %s durante a corrida, e nenhuma "+
			"carrega o id dela. Este Redis e compartilhado de proposito, e um inventario antes e depois "+
			"nao diz quem escreveu: %s", len(strayed), active.prefix, shortList(strayed)))
	}

	final := rep.outcome()
	rep.write(dest.out, final, nil)
	return final, nil
}

// keptNote names what a run started with -keep leaves behind, for whoever cleans it up.
func keptNote(active *run, workDir string) string {
	if workDir == "" {
		workDir = "nenhuma pasta criada"
	}
	return "guardado a pedido (-keep): banco " + active.database + ", prefixo " + active.prefix + ", " + workDir
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

// shortList keeps a report line readable when the list is long, and says how many it left
// out rather than trimming in silence.
func shortList(keys []string) string {
	if len(keys) <= 10 {
		return fmt.Sprint(keys)
	}
	return fmt.Sprintf("%v e mais %d", keys[:10], len(keys)-10)
}

// stoppedOutcome is the code a run that stopped early exits on.
//
// Two rules, and the order between them is the point. A run that gave up because it had
// already found a broken lease is a defect, so it exits on what the report says rather
// than on "the machine was not ready" -- filing the clearest red in the bench under the
// category a reader skips is how it stops being read. And a violation already in the
// report outranks a reading that failed after it: the assertions run before the last Redis
// inspection, so a connection dropping in between would otherwise make the printed report,
// which shows the QUEBRADO, disagree with the number the run exits on. What failed last
// does not decide what was already found.
func stoppedOutcome(rep *report, err error) outcome {
	if rep.outcome() == outcomeInvariant {
		return outcomeInvariant
	}
	if errors.Is(err, errInvariantBroken) {
		return rep.outcome()
	}
	return outcomeSetup
}
