package runn

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/k1LoW/runn/internal/fs"
	"github.com/k1LoW/runn/internal/store"
	"github.com/samber/lo"
)

type result string

const (
	resultSuccess result = "success"
	resultFailure result = "failure"
	resultSkipped result = "skipped"
)

// RunResult is the result of a runbook run.
type RunResult struct {
	ID          string        // Runbook ID
	Desc        string        // Description of runbook
	Labels      []string      // Labels of runbook
	Path        string        // Path of runbook
	Skipped     bool          // Whether runbook run was skipped or not
	Err         error         // Error during runbook run.
	StepResults []*StepResult // Step results of runbook run
	Elapsed     time.Duration // Elapsed time of runbook run
	store       *store.Store  // Store of runbook run
	included    bool          // Whether runbook is included or not
}

// StepResult is the result of a step run.
type StepResult struct {
	ID                 string        // Runbook ID
	Index              int           // Index of step
	Key                string        // Key of step
	Desc               string        // Description of step
	RunnerType         RunnerType    // Runner type of step
	RunnerKey          string        // Runner key of step
	Skipped            bool          // Whether step run was skipped or not
	Err                error         // Error during step run.
	IncludedRunResults []*RunResult  // Run results of runbook loaded by include runner
	Elapsed            time.Duration // Elapsed time of step run
}

type runNResult struct {
	Total      atomic.Int64
	RunResults []*RunResult
	mu         sync.Mutex
}

type runNResultSimplified struct {
	Total   int64                  `json:"total"`
	Success int64                  `json:"success"`
	Failure int64                  `json:"failure"`
	Skipped int64                  `json:"skipped"`
	Results []*runResultSimplified `json:"results"`
	Elapsed time.Duration          `json:"elapsed,omitempty"`
}

type runResultSimplified struct {
	ID      string                  `json:"id"`
	Desc    string                  `json:"desc,omitempty"`
	Labels  []string                `json:"labels,omitempty"`
	Path    string                  `json:"path"`
	Result  result                  `json:"result"`
	Steps   []*stepResultSimplified `json:"steps"`
	Elapsed time.Duration           `json:"elapsed,omitempty"`
}

type stepResultSimplified struct {
	ID                 string                 `json:"id"`
	Index              int                    `json:"index"`
	Key                string                 `json:"key"`
	Desc               string                 `json:"desc,omitempty"`
	RunnerType         RunnerType             `json:"runner_type,omitempty"`
	RunnerKey          string                 `json:"runner_key,omitempty"`
	Result             result                 `json:"result"`
	Error              *stepErrorSimplified   `json:"error,omitempty"`
	IncludedRunResults []*runResultSimplified `json:"included_run_result,omitempty"`
	Elapsed            time.Duration          `json:"elapsed,omitempty"`
}

type stepErrorSimplified struct {
	Message   string `json:"message"`
	Condition string `json:"condition,omitempty"`
	ExprTrace string `json:"expr_trace,omitempty"`
}

func newRunResult(desc string, labels []string, path string, included bool, store *store.Store) *RunResult {
	return &RunResult{
		Desc:     desc,
		Labels:   labels,
		Path:     path,
		included: included,
		store:    store,
	}
}

// HasFailure returns true if any run result has failure.
func (r *runNResult) HasFailure() bool {
	for _, rr := range r.RunResults {
		if rr.Err != nil {
			return true
		}
	}
	return false
}

func (r *runNResult) Out(out io.Writer) error {
	var ts, fs string
	_, _ = fmt.Fprintln(out, "")
	if r.HasFailure() {
		_, _ = fmt.Fprintln(out, "")
		i := 1
		var err error
		for _, rr := range r.RunResults {
			i, err = rr.outFailure(out, i)
			if err != nil {
				return err
			}
		}
	}
	_, _ = fmt.Fprintln(out, "")

	rs := r.simplify()
	if rs.Total == 1 {
		ts = fmt.Sprintf("%d scenario", rs.Total)
	} else {
		ts = fmt.Sprintf("%d scenarios", rs.Total)
	}
	ss := fmt.Sprintf("%d skipped", rs.Skipped)
	if rs.Failure == 1 {
		fs = fmt.Sprintf("%d failure", rs.Failure)
	} else {
		fs = fmt.Sprintf("%d failures", rs.Failure)
	}
	if r.HasFailure() {
		if _, err := fmt.Fprintf(out, red("%s, %s, %s\n"), ts, ss, fs); err != nil {
			return err
		}
	} else {
		if _, err := fmt.Fprintf(out, green("%s, %s, %s\n"), ts, ss, fs); err != nil {
			return err
		}
	}
	return nil
}

func (r *runNResult) OutJSON(out io.Writer) error {
	s := r.simplify()
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	if _, err := out.Write(b); err != nil {
		return err
	}
	if _, err := fmt.Fprint(out, "\n"); err != nil {
		return err
	}
	return nil
}

func (rr *RunResult) OutFailure(out io.Writer) error {
	_, err := rr.outFailure(out, 1)
	return err
}

func (rr *RunResult) Store() map[string]any {
	return rr.store.ToMap()
}

func (r *runNResult) simplify() runNResultSimplified {
	s := runNResultSimplified{
		Total: r.Total.Load(),
	}
	for _, rr := range r.RunResults {
		switch {
		case rr.Err != nil:
			s.Failure += 1
		case rr.Skipped:
			s.Skipped += 1
		default:
			s.Success += 1
		}
		s.Results = append(s.Results, simplifyRunResult(rr))
	}
	return s
}

func (rr *RunResult) outFailure(out io.Writer, index int) (int, error) {
	if rr.Err == nil {
		return index, nil
	}
	stacks, errs := failedRunbookStacksAndErrors(rr)
	for ii, frames := range stacks {
		_, _ = fmt.Fprintf(out, "%d) %s %s\n", index, normalizePath(frames[0].path), cyan(rr.ID))
		if cs := formatCallStack(frames); cs != "" {
			_, _ = fmt.Fprint(out, sprintMultilinef("  %s\n", "%v", cs))
		}
		_, _ = fmt.Fprint(out, sprintMultilinef("  %s\n", "%v", red(fmt.Sprintf("Failure/Error: %s", strings.TrimRight(errs[ii].Error(), "\n")))))

		last := frames[len(frames)-1].path
		b, err := fs.ReadFile(last)
		if err != nil {
			return index, err
		}

		idx := frames[len(frames)-1].stepIndex()
		if idx >= 0 {
			picked, err := pickStepYAML(string(b), idx)
			if err != nil {
				return index, err
			}
			_, _ = fmt.Fprintf(out, "  Failure step (%s):\n", normalizePath(last))
			_, _ = fmt.Fprint(out, sprintMultilinef("  %s\n", "%v", picked))
			_, _ = fmt.Fprintln(out, "")
		}

		index++
	}
	return index, nil
}

// failureFrame is one level of the include chain that leads to a failure.
type failureFrame struct {
	path string
	// step is nil when the failure cannot be attributed to a step of the runbook.
	step *StepResult
}

func (f failureFrame) stepIndex() int {
	if f.step == nil {
		return -1
	}
	return f.step.Index
}

func (f failureFrame) String() string {
	// Inline included runbooks get a synthetic path derived from the file that defines them.
	if file, _, ok := strings.Cut(f.path, ".inline-0x"); ok {
		loc := normalizePath(file) + " (inline include)"
		if f.step == nil {
			return loc
		}
		return fmt.Sprintf("%s steps[%d]", loc, f.step.Index)
	}
	loc := normalizePath(f.path)
	if f.step == nil {
		return loc
	}
	if line := stepStartLine(f.path, f.step.Index); line > 0 {
		loc = fmt.Sprintf("%s:%d", loc, line)
	}
	if f.step.Key == strconv.Itoa(f.step.Index) {
		loc = fmt.Sprintf("%s steps[%d]", loc, f.step.Index)
	} else {
		loc = fmt.Sprintf("%s steps.%s", loc, f.step.Key)
	}
	if f.step.Desc != "" {
		loc = fmt.Sprintf("%s %q", loc, f.step.Desc)
	}
	return loc
}

// stepStartLine returns 0 if the line of the step cannot be determined.
func stepStartLine(path string, idx int) int {
	b, err := fs.ReadFile(path)
	if err != nil {
		return 0
	}
	a := detectRunbookAreas(string(b))
	if idx < 0 || idx >= len(a.Steps) {
		return 0
	}
	return a.Steps[idx].Start.Line
}

// formatCallStack renders the include chain from the root runbook down to the failed step.
// It returns an empty string if no frame can be attributed to a step.
func formatCallStack(frames []failureFrame) string {
	if !slices.ContainsFunc(frames, func(f failureFrame) bool { return f.step != nil }) {
		return ""
	}
	const tr = "└── "
	lines := []string{"Call stack:"}
	for i, f := range frames {
		var branch string
		if i > 0 {
			branch = strings.Repeat("    ", i-1) + tr
		}
		lines = append(lines, "  "+branch+f.String())
	}
	return strings.Join(lines, "\n")
}

// failedRunbookStacksAndErrors returns, for each failure, the include chain
// from rr down to the failed step together with the error of that step.
func failedRunbookStacksAndErrors(rr *RunResult) ([][]failureFrame, []error) {
	var (
		stacks [][]failureFrame
		errs   []error
	)
	if rr.Err == nil {
		return stacks, errs
	}
	for _, sr := range rr.StepResults {
		if sr == nil || sr.Err == nil {
			continue
		}
		frame := failureFrame{path: rr.Path, step: sr}
		if len(sr.IncludedRunResults) == 0 {
			stacks = append(stacks, []failureFrame{frame})
			errs = append(errs, sr.Err)
			continue
		}
		for _, ir := range sr.IncludedRunResults {
			ss, es := failedRunbookStacksAndErrors(ir)
			for _, s := range ss {
				stacks = append(stacks, append([]failureFrame{frame}, s...))
			}
			errs = append(errs, es...)
		}
	}
	if len(stacks) == 0 {
		stacks = append(stacks, []failureFrame{{path: rr.Path}})
		errs = append(errs, rr.Err)
	}
	return stacks, errs
}

func simplifyRunResult(rr *RunResult) *runResultSimplified {
	if rr == nil {
		return nil
	}
	var r result
	switch {
	case rr.Err != nil:
		r = resultFailure
	case rr.Skipped:
		r = resultSkipped
	default:
		r = resultSuccess
	}
	return &runResultSimplified{
		ID:      rr.ID,
		Desc:    rr.Desc,
		Labels:  rr.Labels,
		Path:    normalizePath(rr.Path),
		Result:  r,
		Steps:   simplifyStepResults(rr.StepResults),
		Elapsed: rr.Elapsed,
	}
}

func simplifyStepResults(stepResults []*StepResult) []*stepResultSimplified {
	var simplified []*stepResultSimplified
	for _, sr := range stepResults {
		var r result
		switch {
		case sr.Err != nil:
			r = resultFailure
		case sr.Skipped:
			r = resultSkipped
		default:
			r = resultSuccess
		}
		s := &stepResultSimplified{
			ID:         sr.ID,
			Index:      sr.Index,
			Key:        sr.Key,
			Desc:       sr.Desc,
			RunnerType: sr.RunnerType,
			RunnerKey:  sr.RunnerKey,
			Result:     r,
			IncludedRunResults: lo.Map(sr.IncludedRunResults, func(ir *RunResult, _ int) *runResultSimplified {
				return simplifyRunResult(ir)
			}),
			Elapsed: sr.Elapsed,
		}
		if sr.Err != nil {
			se := &stepErrorSimplified{
				Message: sr.Err.Error(),
			}
			var cfe *condFalseError
			if errors.As(sr.Err, &cfe) {
				se.Condition = cfe.cond
				se.ExprTrace = cfe.tree
			}
			s.Error = se
		}
		simplified = append(simplified, s)
	}
	return simplified
}

func sprintMultilinef(lineformat, format string, a ...any) string {
	lines := strings.Split(fmt.Sprintf(format, a...), "\n")
	var sb strings.Builder
	for _, l := range lines {
		fmt.Fprintf(&sb, lineformat, l)
	}
	return sb.String()
}

var (
	// root = project root path.
	root string
	once sync.Once
)

func normalizePath(p string) string {
	once.Do(func() {
		root, _ = projectRoot()
	})
	if root == "" {
		return p
	}
	abs, err := filepath.Abs(filepath.Clean(p))
	if err != nil {
		return p
	}
	rel, err := filepath.Rel(root, abs)
	if err != nil {
		return p
	}
	return rel
}

func projectRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if dir == filepath.Dir(dir) {
			return "", errors.New("failed to find project root")
		}
		gitPath := filepath.Join(dir, ".git")
		fi, err := os.Stat(gitPath)
		if err == nil {
			// .git is a directory (normal repo) or a file (worktree)
			if fi.IsDir() || fi.Mode().IsRegular() {
				return dir, nil
			}
		}
		dir = filepath.Dir(dir)
	}
}
