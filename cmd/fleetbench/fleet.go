package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// A fleet of real processes, built from the tree this bench lives in.
//
// Built, and not found. A bench that runs whatever `connector` the PATH happens to hold
// measures a binary nobody chose: a stale `bin/`, an image from last week, the one the
// last round left behind. Every mutation proof downstream then passes by construction,
// because the mutant never reached a process. So the binary is compiled here, from this
// worktree, and its path and hash are printed -- the hash is what makes "the mutant
// arrived" checkable rather than assumed.
type fleet struct {
	binary    string
	binarySum string
	dir       string
	env       map[string]string
	instances []*instance
}

type instance struct {
	name     string
	httpAddr string
	pid      int
	logPath  string
	cmd      *exec.Cmd
	log      *os.File

	// reaped says this pid has already been waited for, so it names nothing any more and
	// must not be signalled again. See gone().
	reaped bool
}

// buildConnector compiles cmd/connector out of the module this bench belongs to.
//
// `go build` rather than `go run`: the bench has to hold a file it can hash and name, and
// `go run` leaves the binary in a cache directory it does not report. The marker a
// mutation plants has to be findable in something, and "the binary at this path, with this
// hash" is the smallest something that answers it.
func buildConnector(ctx context.Context, moduleDir, outDir string) (binaryPath, sha256Hex string, err error) {
	binary := filepath.Join(outDir, "connector")
	// The module this bench belongs to, which under a mutation run is the mutated copy.
	// The path comes from walking up to `go.mod`, never from a caller.
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/connector") //nolint:gosec // the arguments are fixed; only the directory varies
	build.Dir = moduleDir
	build.Env = os.Environ()
	if out, err := build.CombinedOutput(); err != nil {
		return "", "", fmt.Errorf("build the connector under test from %s: %w\n%s", moduleDir, err, out)
	}
	file, err := os.Open(binary) //nolint:gosec // the path is the one this function just built
	if err != nil {
		return "", "", fmt.Errorf("open the connector just built: %w", err)
	}
	defer func() { _ = file.Close() }()
	sum := sha256.New()
	if _, err := io.Copy(sum, file); err != nil {
		return "", "", fmt.Errorf("hash the connector just built: %w", err)
	}
	return binary, hex.EncodeToString(sum.Sum(nil)), nil
}

// start runs one connector process and waits until it answers /healthz.
//
// Every process gets its own log file and its own HTTP port, and the PID is kept: the
// cleanup at the end of a run kills what this bench started, by the pid it captured, and
// nothing else. Killing by name would reach every connector on the machine, including the
// ones another session is measuring.
func (f *fleet) start(ctx context.Context, name string) (*instance, error) {
	port, err := freePort(ctx)
	if err != nil {
		return nil, err
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)
	logPath := filepath.Join(f.dir, "connector-"+name+".log")
	logFile, err := os.Create(logPath) //nolint:gosec // the path is this run's own directory plus the instance name
	if err != nil {
		return nil, fmt.Errorf("open the log of %s: %w", name, err)
	}

	// Bound to the run's context, so an interrupt takes the fleet with it instead of
	// leaving connectors behind holding leases on a database the run is about to drop.
	cmd := exec.CommandContext(ctx, f.binary, "serve") //nolint:gosec // the binary is the one this run built, and "serve" is fixed
	cmd.Dir = f.dir
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Env = fleetEnv(os.Environ(), f.env, name, addr)
	// Its own process group, so that a kill reaches the connector and whatever it started,
	// and never reaches this bench or the shell that ran it.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("start %s: %w", name, err)
	}

	live := &instance{name: name, httpAddr: addr, pid: cmd.Process.Pid, logPath: logPath, cmd: cmd, log: logFile}
	f.instances = append(f.instances, live)
	if err := live.waitHealthy(ctx, 30*time.Second); err != nil {
		return live, err
	}
	return live, nil
}

func (i *instance) waitHealthy(ctx context.Context, within time.Duration) error {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// A deadline per request, and not only between them. A connector that accepts the
		// connection and then stops answering leaves `Do` waiting forever on a context
		// that has none, and the loop below never gets to look at its own deadline again:
		// the 30 seconds this function promises would never fire.
		attempt, cancelAttempt := context.WithTimeout(ctx, 5*time.Second)
		request, _ := http.NewRequestWithContext(attempt, http.MethodGet, "http://"+i.httpAddr+"/healthz", http.NoBody)
		response, err := http.DefaultClient.Do(request)
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			healthy := response.StatusCode == http.StatusOK
			cancelAttempt()
			if healthy {
				return nil
			}
		} else {
			cancelAttempt()
		}
		if i.cmd.ProcessState != nil {
			return fmt.Errorf("%s exited before it was healthy; its log is at %s", i.name, i.logPath)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("%s never answered /healthz within %s; its log is at %s", i.name, within, i.logPath)
}

// kill stops one instance the way a machine losing power stops it: no signal it can
// handle, no chance to hand anything back. That is the case the handover phase is about.
func (i *instance) kill() error {
	if i.gone() {
		return nil
	}
	if err := syscall.Kill(-i.pid, syscall.SIGKILL); err != nil && !strings.Contains(err.Error(), "no such process") {
		return fmt.Errorf("kill %s (pid %d): %w", i.name, i.pid, err)
	}
	_, _ = i.cmd.Process.Wait()
	i.reaped = true
	return nil
}

// gone reports whether this instance has already been waited for, and it is what every
// signal in this file is guarded by.
//
// A pid stops naming a process the moment it is reaped, and the number is then free for
// the kernel to hand to somebody else. This bench kills two instances in the middle of a
// run and shuts the fleet down minutes later, so without this the shutdown would send
// SIGCONT and SIGTERM to two pids that have been dead since then -- to a process GROUP,
// at that, which on a machine running several of these at once is somebody else's work
// with no warning. `cmd.Process` stays non-nil after a wait, so it cannot answer this on
// its own.
func (i *instance) gone() bool {
	return i.cmd == nil || i.cmd.Process == nil || i.reaped
}

// freeze stops an instance without ending it, and thaw lets it go on.
//
// The pair exists because a kill cannot produce the case invariant 1's fence is written
// against. A killed owner publishes nothing more, so "an instance that lost the lease
// stops at once" is asserted over a series where it could not have failed: the mutant that
// removes the fence comes out green, which is the bench saying it measured something it
// never reached.
//
// A frozen owner is a live one that stopped renewing. Its lease expires on the server, a
// peer takes the account and publishes under a higher epoch, and when the freeze is lifted
// the old owner wakes up holding work it was in the middle of. Whether it then publishes
// is exactly the fence.
//
// By the pid this run captured, to its own process group, and never by a pattern over the
// process name: this machine runs other people's connectors.
func (i *instance) freeze() error {
	if i.gone() {
		return nil
	}
	if err := syscall.Kill(-i.pid, syscall.SIGSTOP); err != nil {
		return fmt.Errorf("freeze %s (pid %d): %w", i.name, i.pid, err)
	}
	return nil
}

func (i *instance) thaw() error {
	if i.gone() {
		return nil
	}
	if err := syscall.Kill(-i.pid, syscall.SIGCONT); err != nil {
		return fmt.Errorf("thaw %s (pid %d): %w", i.name, i.pid, err)
	}
	return nil
}

// stop ends an instance the way an operator does, and waits for it.
func (i *instance) stop() {
	if !i.gone() {
		// Continued first: a process left frozen never sees the SIGTERM, and the wait
		// below would spend its ten seconds before falling back to the kill.
		_ = syscall.Kill(-i.pid, syscall.SIGCONT)
		_ = syscall.Kill(-i.pid, syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _, _ = i.cmd.Process.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = syscall.Kill(-i.pid, syscall.SIGKILL)
			<-done
		}
		i.reaped = true
	}
	if i.log != nil {
		_ = i.log.Close()
	}
}

// shutdown ends every process this bench started, by the pid it captured when it started
// it. Never by a pattern over the process name: this machine runs other people's
// connectors, and a pattern that matches "connector" matches all of them.
func (f *fleet) shutdown() {
	for _, live := range f.instances {
		live.stop()
	}
}

// metrics reads one instance's /metrics and returns the samples it asked for, by name.
//
// Every name in `want` has to be there. Absence is an error and not a zero, because for an
// unlabelled metric the two say opposite things: `wac_sessions_running` reads zero when the
// instance runs nothing, and is absent only when nobody registered it.
func (i *instance) metrics(ctx context.Context, want ...string) (map[string]float64, error) {
	return i.readMetrics(ctx, want, nil)
}

// metricsWithOptional is metrics for the case where absence is a reading and not a gap.
//
// A `CounterVec` with no children is absent from the exposition entirely -- there is no
// zero row to serve -- so "this instance reclaimed nothing" and "this build counts
// nothing" look identical from outside. What tells them apart is a plain counter beside
// it: `wac_command_reclaim_passes_total` is written on every completed pass whatever the
// pass found, so a run with passes and no `wac_commands_reclaimed_total` is a fleet that
// looked and found nothing. That counter belongs in `want`, and the Vec in `optional`.
func (i *instance) metricsWithOptional(ctx context.Context, want, optional []string) (map[string]float64, error) {
	return i.readMetrics(ctx, want, optional)
}

func (i *instance) readMetrics(ctx context.Context, want, optional []string) (map[string]float64, error) {
	// Bounded on its own, for the reason the health probe is: a connector that accepts
	// the connection and answers nothing would otherwise hold this call forever, and every
	// deadline above it -- adoption, drain, the frozen phase -- is checked only between
	// readings.
	read, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(read, http.MethodGet, "http://"+i.httpAddr+"/metrics", http.NoBody)
	if err != nil {
		return nil, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("read /metrics of %s: %w", i.name, err)
	}
	defer func() { _ = response.Body.Close() }()
	// The status is checked, and a metric that is not there is an error rather than a
	// zero. Both are the same mistake: a reading that failed, spent as a number. A 404
	// parsed as "no samples" and an absent gauge read as "no sessions" would each turn a
	// bench that cannot see the fleet into a bench reporting an empty one.
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("/metrics of %s answered %s", i.name, response.Status)
	}

	wanted := map[string]bool{}
	for _, name := range want {
		wanted[name] = true
	}
	for _, name := range optional {
		wanted[name] = true
	}
	found := map[string]float64{}
	scanner := bufio.NewScanner(response.Body)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, rest, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		if brace := strings.IndexByte(name, '{'); brace >= 0 {
			name = name[:brace]
		}
		if !wanted[name] {
			continue
		}
		value, err := strconv.ParseFloat(strings.Fields(rest)[0], 64)
		if err != nil {
			continue
		}
		// Summed, because a metric with labels arrives as several samples and what the
		// bench asks of `wac_sessions_running` is the instance's total.
		found[name] += value
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read /metrics of %s: %w", i.name, err)
	}
	for _, name := range want {
		if _, there := found[name]; !there {
			return nil, fmt.Errorf("/metrics of %s carries no %s at all, so there is no number to "+
				"read: a metric that is missing is not a metric that is zero", i.name, name)
		}
	}
	// The optional ones are filled in as zero once they are known to be absent, which is
	// what the caller asked for by putting them there.
	for _, name := range optional {
		if _, there := found[name]; !there {
			found[name] = 0
		}
	}
	return found, nil
}

func freePort(ctx context.Context) (int, error) {
	var config net.ListenConfig
	listener, err := config.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("find a free port for a connector: %w", err)
	}
	defer func() { _ = listener.Close() }()
	tcp, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("the listener came back on %s, which names no port", listener.Addr())
	}
	return tcp.Port, nil
}

// fleetEnv is the environment a connector of this run is started with, and every WAC_
// variable in it comes from the run.
//
// MEASURED as a real hazard, not a tidiness rule: `app.New` opens the media store whenever
// `WAC_MEDIA_ROOT` is set, with the fake engine like with any other, and `sweepBlobs` then
// deletes expired blobs under that directory once a minute. A shell that exports the
// variable for a connector somebody is running by hand would hand this run four processes
// sweeping that connector's blobs -- and `media.Store` only knows about writes from its own
// process, so a blob being written elsewhere is a blob this run can delete under it.
//
// The isolation of this bench is by identifier everywhere else (a database named after the
// run, a key prefix named after the run, processes killed by captured pids), and an
// inherited variable is the one door left open in it. So the WAC_ half of the environment
// is dropped wholesale rather than one name at a time: the next variable the connector
// learns to read would otherwise arrive here inherited and unnoticed. Everything that is
// not WAC_ is passed through, because a connector still needs PATH, HOME and the rest.
func fleetEnv(environ []string, own map[string]string, name, addr string) []string {
	out := make([]string, 0, len(environ)+len(own)+2)
	for _, entry := range environ {
		if strings.HasPrefix(entry, "WAC_") {
			continue
		}
		out = append(out, entry)
	}
	out = append(out, "WAC_INSTANCE="+name, "WAC_HTTP_ADDR="+addr)
	for key, value := range own {
		out = append(out, key+"="+value)
	}
	return out
}
