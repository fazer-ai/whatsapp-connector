package main

import (
	"fmt"
	"io"
	"strings"
)

// What this bench prints, and the line it keeps between two kinds of statement.
//
// An ASSERTION has a verdict and a series: it says what it checked, over how many points,
// and whether it held. A MEASUREMENT has a number and no verdict: nothing about it passes
// or fails, because a capacity figure is a fact about this machine on this day.
//
// They are separated because merging them is how a bad number becomes a failure somebody
// learns to ignore, and then a broken invariant becomes "that number again". The three
// outcomes this bench can end in have three exit codes for the same reason: a script reads
// the code, not the prose.
type report struct {
	engine    string
	binary    string
	binarySum string
	processes []processNote

	assertions   []*assertion
	measurements []measurement
	notes        []string
}

// assertion is a claim with a verdict and the size of what it looked at.
//
// `series` is not decoration. An order assertion over one point cannot fail, and an
// assertion over an empty series is not an assertion at all -- it is the absence of one,
// printed in the shape of a pass. `held` is therefore only meaningful next to `series`,
// and `state` collapses the two into the word that gets printed.
type assertion struct {
	invariant string // the operational invariant this speaks for, in words
	claim     string // what was checked
	series    string // how many points, named: "412 events over 37 (sid, epoch) pairs"
	points    int
	held      bool
	detail    string // the evidence, when it did not hold
	notWhy    string // why it was not measured, when it was not
}

func (a *assertion) state() string {
	switch {
	case a.notWhy != "":
		return "NAO MEDIDO"
	case a.points == 0:
		return "NAO MEDIDO"
	case a.held:
		return "AFIRMADO"
	default:
		return "QUEBRADO"
	}
}

// measurement is a number with a unit and no verdict, unless whoever ran the bench
// declared a range for it. The range comes from a flag rather than from this file: a
// threshold nobody asked for is an invented expectation, and it would turn a machine
// having a slow afternoon into a defect report.
type measurement struct {
	phase    string
	name     string
	value    float64
	unit     string
	expected string // empty unless a range was declared
	outside  bool
}

type processNote struct {
	instance string
	pid      int
	httpAddr string
	logPath  string
}

func (r *report) assert(a *assertion) { r.assertions = append(r.assertions, a) }

func (r *report) measure(phase, name string, value float64, unit string) {
	r.measurements = append(r.measurements, measurement{phase: phase, name: name, value: value, unit: unit})
}

// note records a limit, a decision or a fact about this run that has no verdict and no
// number. It takes a finished string rather than a format: a note built at the call site
// is a note whose arguments the compiler has already checked.
func (r *report) note(text string) { r.notes = append(r.notes, text) }

// outcome is what the exit code says, and the three are kept apart deliberately.
type outcome int

const (
	outcomeGreen     outcome = 0 // every assertion held
	outcomeInvariant outcome = 1 // an operational invariant is broken: a defect
	outcomeSetup     outcome = 2 // the machine was not ready: not a defect, and not a pass
	outcomeOutside   outcome = 3 // a measurement fell outside a range somebody declared: not a defect
)

func (r *report) outcome() outcome {
	for _, a := range r.assertions {
		if a.state() == "QUEBRADO" {
			return outcomeInvariant
		}
	}
	for _, m := range r.measurements {
		if m.outside {
			return outcomeOutside
		}
	}
	return outcomeGreen
}

func (o outcome) label() string {
	switch o {
	case outcomeGreen:
		return "VERDE"
	case outcomeInvariant:
		return "INVARIANTE QUEBRADA"
	case outcomeSetup:
		return "SETUP INCOMPLETO"
	case outcomeOutside:
		return "MEDIDA FORA DA FAIXA"
	}
	return "DESCONHECIDO"
}

// write prints the report, once, under the outcome it is told and not under one it works
// out for itself.
//
// Told, because a run that stopped in setup has no verdict to compute: recomputing one
// there printed VERDE over an empty assertion list, directly above the line saying the run
// never got started. Once, because `reason` goes under the label here instead of being
// printed a second time by the caller, and two `=== SETUP INCOMPLETO ===` headers -- one of
// them on stderr -- is how a reader of the log counts two failures in one run.
func (r *report) write(out io.Writer, o outcome, reason error) {
	_, _ = fmt.Fprintf(out, "\n=== bancada de frota · motor %s ===\n", r.engine)
	_, _ = fmt.Fprintf(out, "binario: %s (sha256 %s)\n", r.binary, r.binarySum)
	for _, p := range r.processes {
		_, _ = fmt.Fprintf(out, "processo: WAC_INSTANCE=%s pid=%d http=%s log=%s\n", p.instance, p.pid, p.httpAddr, p.logPath)
	}

	_, _ = fmt.Fprintf(out, "\n--- ASSERCOES (tem veredito) ---\n")
	for _, a := range r.assertions {
		_, _ = fmt.Fprintf(out, "[%s] %s\n    invariante: %s\n    serie: %s\n", a.state(), a.claim, a.invariant, a.series)
		if a.notWhy != "" {
			_, _ = fmt.Fprintf(out, "    nao medido porque: %s\n", a.notWhy)
		}
		if a.detail != "" {
			_, _ = fmt.Fprintf(out, "    evidencia: %s\n", indent(a.detail))
		}
	}

	_, _ = fmt.Fprintf(out, "\n--- MEDICOES (tem numero, nao tem veredito) ---\n")
	byPhase := map[string][]measurement{}
	var phases []string
	for _, m := range r.measurements {
		if _, seen := byPhase[m.phase]; !seen {
			phases = append(phases, m.phase)
		}
		byPhase[m.phase] = append(byPhase[m.phase], m)
	}
	for _, phase := range phases {
		_, _ = fmt.Fprintf(out, "%s:\n", phase)
		for _, m := range byPhase[phase] {
			line := fmt.Sprintf("    %-38s %.2f %s", m.name, m.value, m.unit)
			if m.expected != "" {
				state := "dentro da faixa"
				if m.outside {
					state = "FORA DA FAIXA"
				}
				line += fmt.Sprintf("   (esperado %s: %s)", m.expected, state)
			}
			_, _ = fmt.Fprintln(out, line)
		}
	}

	if len(r.notes) > 0 {
		_, _ = fmt.Fprintf(out, "\n--- LIMITES E NOTAS ---\n")
		for _, n := range r.notes {
			_, _ = fmt.Fprintf(out, "  %s\n", n)
		}
	}

	_, _ = fmt.Fprintf(out, "\n=== %s (exit %d) ===\n", o.label(), int(o))
	if reason != nil {
		_, _ = fmt.Fprintf(out, "%v\n", reason)
	}
}

func indent(text string) string {
	return strings.ReplaceAll(strings.TrimRight(text, "\n"), "\n", "\n               ")
}
