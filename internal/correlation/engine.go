package correlation

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/wyw14/cry-103/internal/platform"
)

// Config tunes the correlation window and OTDR agreement tolerance.
type Config struct {
	// Window is the maximum gap between an event and the case's current time
	// span for them to be aggregated together. Events from different topology
	// generations are never merged regardless of the window.
	Window time.Duration
	// OTDRToleranceKM is the distance within which an OTDR fix must agree with
	// the existing inferred/confirmed position. A wider gap forces field review.
	OTDRToleranceKM float64
}

func (c Config) validate() error {
	if c.Window <= 0 {
		return fmt.Errorf("correlation window must be positive")
	}
	if c.OTDRToleranceKM <= 0 {
		return fmt.Errorf("OTDR tolerance must be positive")
	}
	return nil
}

// Result reports what an ingestion did.
type Result struct {
	Case       Snapshot       `json:"case"`
	Duplicate  bool           `json:"duplicate"`
	Created    bool           `json:"created"`
	Appended   bool           `json:"appended"`
	LocationTo LocationStatus `json:"location_status,omitempty"`
}

// Engine correlates events into fault cases. It is safe for concurrent use.
type Engine struct {
	mu    sync.RWMutex
	cfg   Config
	ids   platform.IDGenerator
	now   func() time.Time
	cases map[string]*Case
	// eventIndex maps spanID|generation|eventID to the case that owns it,
	// guaranteeing a repeated alarm is folded into its original case instead
	// of opening a parallel one.
	eventIndex map[string]string
}

func NewEngine(ids platform.IDGenerator, now func() time.Time, cfg Config) (*Engine, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Engine{
		cfg: cfg, ids: ids, now: now,
		cases:      make(map[string]*Case),
		eventIndex: make(map[string]string),
	}, nil
}

func eventKey(spanID string, generation uint64, eventID string) string {
	return fmt.Sprintf("%s\x00%d\x00%s", spanID, generation, eventID)
}

// Ingest accepts one multi-source event and merges it into the matching case,
// opening a new case when no open case matches the line, generation and time
// window. A repeated event id is returned as a duplicate and never appends
// evidence nor creates a parallel case.
func (e *Engine) Ingest(event Event) (Result, error) {
	if err := event.Validate(); err != nil {
		return Result{}, fmt.Errorf("validate correlation event: %w", err)
	}
	event.Observed = event.UTC()

	e.mu.Lock()
	defer e.mu.Unlock()

	key := eventKey(event.SpanID, event.Generation, event.ID)
	if caseID, exists := e.eventIndex[key]; exists {
		current := e.cases[caseID]
		return Result{Case: current.snapshot(), Duplicate: true, LocationTo: current.Location.Status}, nil
	}

	target := e.findMatch(event)
	created := false
	if target == nil {
		target = e.openCase(event)
		created = true
		target.recordState("", StateOpen, "", fmt.Sprintf("case opened from %s evidence %s", event.Source, event.ID), e.now().UTC())
	}
	beforeState := target.State
	beforeLocation := target.Location.Status
	target.append(event, e.now().UTC())
	e.eventIndex[key] = target.ID
	e.applyLocation(target, event)
	if target.Location.Status == LocationFieldReview {
		target.State = StateReviewing
	}
	if target.Location.Status != beforeLocation {
		target.recordState(beforeState, target.State, target.Location.Status,
			fmt.Sprintf("location %s from %s evidence", target.Location.Status, event.Source), e.now().UTC())
	}
	return Result{
		Case: target.snapshot(), Created: created, Appended: true, LocationTo: target.Location.Status,
	}, nil
}

func (event Event) UTC() time.Time {
	return event.Observed.UTC()
}

// findMatch returns the still-open case on the same line and generation whose
// time window overlaps the event. Closed cases never match: a later event is a
// new fault and gets its own case.
func (e *Engine) findMatch(event Event) *Case {
	for _, candidate := range e.cases {
		if candidate.State == StateClosed {
			continue
		}
		if candidate.SpanID != event.SpanID || candidate.Generation != event.Generation {
			continue
		}
		earliest := candidate.FirstObserved.Add(-e.cfg.Window)
		latest := candidate.LastObserved.Add(e.cfg.Window)
		if event.Observed.Before(earliest) || event.Observed.After(latest) {
			continue
		}
		return candidate
	}
	return nil
}

func (e *Engine) openCase(event Event) *Case {
	at := e.now().UTC()
	c := &Case{
		ID: e.ids.New("case"), SpanID: event.SpanID, Generation: event.Generation,
		State: StateOpen, Severity: event.Severity, OpenedAt: at, UpdatedAt: at,
		FirstObserved: event.Observed, LastObserved: event.Observed,
	}
	e.cases[c.ID] = c
	return c
}

func (c *Case) append(event Event, at time.Time) {
	if event.Severity > c.Severity {
		c.Severity = event.Severity
	}
	if event.Observed.Before(c.FirstObserved) {
		c.FirstObserved = event.Observed
	}
	if event.Observed.After(c.LastObserved) {
		c.LastObserved = event.Observed
	}
	c.Evidence = append(c.Evidence, Evidence{
		Seq: len(c.Evidence) + 1, EventID: event.ID, Source: event.Source, AssetID: event.AssetID,
		Kind: event.Kind, Severity: event.Severity, DistanceKM: copyDistance(event.DistanceKM), Confidence: event.Confidence,
		Summary: event.Summary, AddedAt: at, Observed: event.Observed,
	})
	c.UpdatedAt = at
}

func copyDistance(value *float64) *float64 {
	if value == nil {
		return nil
	}
	distance := *value
	return &distance
}

// applyLocation advances the location state machine. Indirect sources can only
// infer; an OTDR fix confirms when it agrees with the current position and
// forces field review when it contradicts it. Case state is reconciled by the
// caller after this returns.
func (e *Engine) applyLocation(c *Case, event Event) {
	switch event.Source {
	case SourceOTDRScan:
		if !event.HasDistance() {
			return
		}
		distance := *event.DistanceKM
		switch c.Location.Status {
		case "":
			c.Location = Location{Status: LocationConfirmed, DistanceKM: &distance, Basis: event.ID, Accuracy: event.Confidence}
		case LocationInferred:
			if c.Location.DistanceKM == nil || e.agree(c.Location.DistanceKM, distance) {
				c.Location = Location{Status: LocationConfirmed, DistanceKM: &distance, Basis: event.ID, Accuracy: event.Confidence}
			} else {
				c.Location = Location{
					Status: LocationFieldReview, DistanceKM: c.Location.DistanceKM, Basis: c.Location.Basis,
					Review: fmt.Sprintf("OTDR fix %.2fkm disagrees with inferred %.2fkm", distance, *c.Location.DistanceKM),
				}
			}
		case LocationConfirmed:
			if !e.agree(c.Location.DistanceKM, distance) {
				c.Location = Location{
					Status: LocationFieldReview, DistanceKM: c.Location.DistanceKM, Basis: c.Location.Basis,
					Review: fmt.Sprintf("OTDR fix %.2fkm disagrees with confirmed %.2fkm", distance, *c.Location.DistanceKM),
				}
			} else if event.Confidence > c.Location.Accuracy {
				c.Location.DistanceKM = &distance
				c.Location.Basis = event.ID
				c.Location.Accuracy = event.Confidence
			}
		case LocationFieldReview:
			// Remote disagreement is already on record; only explicit field
			// verification can restore a confirmed location.
			if c.Location.Review == "" {
				c.Location.Review = "additional OTDR fix while awaiting field verification"
			}
		}
	case SourceRepeaterAlarm, SourceOpticalPower:
		if c.Location.Status == LocationConfirmed || c.Location.Status == LocationFieldReview {
			return
		}
		if !event.HasDistance() {
			if c.Location.Status == "" {
				c.Location = Location{Status: LocationInferred}
			}
			return
		}
		distance := *event.DistanceKM
		if c.Location.Status == "" || c.Location.DistanceKM == nil || event.Confidence > c.Location.Accuracy {
			c.Location = Location{Status: LocationInferred, DistanceKM: &distance, Basis: event.ID, Accuracy: event.Confidence}
		}
	case SourceRepair:
		// Repair operations are evidence of action, never a position fix.
	}
}

func (e *Engine) agree(existing *float64, candidate float64) bool {
	if existing == nil {
		return true
	}
	difference := *existing - candidate
	if difference < 0 {
		difference = -difference
	}
	return difference <= e.cfg.OTDRToleranceKM
}

func (c *Case) recordState(from, to CaseState, location LocationStatus, reason string, at time.Time) {
	c.Transitions = append(c.Transitions, Transition{From: from, To: to, Location: location, Reason: reason, At: at})
	c.UpdatedAt = at
}

// RequestFieldReview manually flags the case for on-site verification.
func (e *Engine) RequestFieldReview(caseID, reason string) (Snapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	c, err := e.active(caseID)
	if err != nil {
		return Snapshot{}, err
	}
	from := c.State
	c.State = StateReviewing
	c.Location.Status = LocationFieldReview
	if reason != "" {
		c.Location.Review = reason
	}
	c.recordState(from, StateReviewing, LocationFieldReview, reason, e.now().UTC())
	return c.snapshot(), nil
}

// ConfirmLocation records a verified position (typically a field crew finding)
// and moves a reviewing case back to open with a confirmed location.
func (e *Engine) ConfirmLocation(caseID string, distanceKM float64, note string) (Snapshot, error) {
	if distanceKM < 0 {
		return Snapshot{}, fmt.Errorf("confirmed distance must be non-negative")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	c, err := e.active(caseID)
	if err != nil {
		return Snapshot{}, err
	}
	from := c.State
	c.Location = Location{Status: LocationConfirmed, DistanceKM: &distanceKM, Basis: "field:" + note, Accuracy: 1}
	c.State = StateOpen
	reason := "field verification confirmed location"
	if note != "" {
		reason = "field verification: " + note
	}
	c.recordState(from, StateOpen, LocationConfirmed, reason, e.now().UTC())
	return c.snapshot(), nil
}

// Close freezes the case. Its full evidence chain is retained; no further
// events are accepted into it.
func (e *Engine) Close(caseID, note string) (Snapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	c, err := e.active(caseID)
	if err != nil {
		return Snapshot{}, err
	}
	from := c.State
	closedAt := e.now().UTC()
	c.State = StateClosed
	c.ClosedAt = &closedAt
	c.CloseNote = note
	c.recordState(from, StateClosed, c.Location.Status, "case closed: "+note, closedAt)
	return c.snapshot(), nil
}

func (e *Engine) active(caseID string) (*Case, error) {
	c := e.cases[caseID]
	if c == nil {
		return nil, fmt.Errorf("correlation case %s not found", caseID)
	}
	if c.State == StateClosed {
		return nil, fmt.Errorf("correlation case %s is closed", caseID)
	}
	return c, nil
}

// Get returns one case with its complete evidence chain.
func (e *Engine) Get(caseID string) (Snapshot, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	c := e.cases[caseID]
	if c == nil {
		return Snapshot{}, false
	}
	return c.snapshot(), true
}

// List returns cases ordered by opening time, optionally filtered by line and
// state. Closed cases are included so their evidence chains stay auditable.
func (e *Engine) List(spanID string, state CaseState) []Snapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]Snapshot, 0, len(e.cases))
	for _, c := range e.cases {
		if spanID != "" && c.SpanID != spanID {
			continue
		}
		if state != "" && c.State != state {
			continue
		}
		result = append(result, c.snapshot())
	}
	sort.Slice(result, func(i, j int) bool { return result[i].OpenedAt.Before(result[j].OpenedAt) })
	return result
}

func (c *Case) snapshot() Snapshot {
	evidence := make([]Evidence, len(c.Evidence))
	copy(evidence, c.Evidence)
	transitions := make([]Transition, len(c.Transitions))
	copy(transitions, c.Transitions)
	return Snapshot{
		Case: Case{
			ID: c.ID, SpanID: c.SpanID, Generation: c.Generation, State: c.State, Severity: c.Severity,
			Location: c.Location, OpenedAt: c.OpenedAt, UpdatedAt: c.UpdatedAt, ClosedAt: c.ClosedAt,
			CloseNote: c.CloseNote, FirstObserved: c.FirstObserved, LastObserved: c.LastObserved,
			Evidence: evidence, Transitions: transitions,
		},
		EvidenceCount: len(evidence),
	}
}
