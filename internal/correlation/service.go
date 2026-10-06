package correlation

import (
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/cry-103/internal/platform"
)

// pairAgreementKM is how close two landing-station OTDR localisations must be
// to confirm the same fault. It mirrors the agreement bonus in span.Locate.
const pairAgreementKM = 2.0

// Config tunes the correlation session.
type Config struct {
	// Window is the post-resolution duplicate-suppression window. A repeat
	// alarm carrying the same dedup key within the window is recognised as a
	// re-notification of the closed case instead of spawning a new one.
	Window time.Duration
}

func (c Config) validate() error {
	if c.Window <= 0 {
		return fmt.Errorf("correlation window must be positive")
	}
	return nil
}

// Engine correlates events into fault cases. At most one case is open per
// span/generation pair; resolved cases are retained for evidence lookup.
type Engine struct {
	mu     sync.RWMutex
	ids    platform.IDGenerator
	now    func() time.Time
	window time.Duration
	cases  map[string]*Case
	// openCase maps "spanID\x00generation" to the open case ID.
	openCase map[string]string
	// lastClosed maps "spanID\x00generation" to the most recently resolved
	// case, used for duplicate suppression.
	lastClosed map[string]string
}

// NewEngine builds an engine. ids and now follow the platform conventions used
// across the services (injectable clock and identity generator).
func NewEngine(ids platform.IDGenerator, now func() time.Time, cfg Config) (*Engine, error) {
	if now == nil {
		return nil, fmt.Errorf("correlation engine requires a clock")
	}
	if ids == nil {
		return nil, fmt.Errorf("correlation engine requires an ID generator")
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Engine{
		ids: ids, now: now, window: cfg.Window,
		cases: make(map[string]*Case), openCase: make(map[string]string), lastClosed: make(map[string]string),
	}, nil
}

// Outcome describes what happened to an observed event.
type Outcome struct {
	// Case is the resulting case state (nil when the event was ignored
	// because no active case exists for its span generation).
	Case *Case `json:"case,omitempty"`
	// Attached is true when the event was appended to the evidence chain.
	Attached bool `json:"attached"`
	// Duplicate is true when the event was a repeat notification and only
	// the repeat counter moved.
	Duplicate bool `json:"duplicate"`
	// Suppressed is true when the event repeated a condition whose case was
	// resolved inside the correlation window. Nothing is stored.
	Suppressed bool `json:"suppressed"`
}

func caseKey(spanID string, generation uint64) string {
	return fmt.Sprintf("%s\x00%d", spanID, generation)
}

// Observe feeds one event into the engine.
func (e *Engine) Observe(event Event) (Outcome, error) {
	if err := event.Validate(); err != nil {
		return Outcome{}, err
	}
	if event.ReceivedAt.IsZero() {
		event.ReceivedAt = e.now().UTC()
	}
	event.ObservedAt = event.ObservedAt.UTC()

	e.mu.Lock()
	defer e.mu.Unlock()

	key := caseKey(event.SpanID, event.Generation)
	if openID := e.openCase[key]; openID != "" {
		current := e.cases[openID]
		return e.attach(current, event)
	}

	// Activity repeating a condition resolved inside the window never spawns
	// a parallel case: an alarm re-notification carrying a known dedup key,
	// or a post-repair acceptance OTDR scan, is suppressed onto the closed
	// case.
	if closedID := e.lastClosed[key]; closedID != "" {
		closed := e.cases[closedID]
		withinWindow := !event.ObservedAt.After(closed.ResolvedAt.Add(e.window))
		knownAlarm := event.Type == EventRepeaterAlarm && event.DedupKey != "" && closed.hasRepeat(event)
		acceptanceScan := event.Type == EventOTDRScan
		if withinWindow && (knownAlarm || acceptanceScan) {
			snapshot := closed.Snapshot()
			return Outcome{Case: &snapshot, Suppressed: true}, nil
		}
	}

	switch event.Type {
	case EventRepeaterAlarm:
		current := e.open(event)
		return e.attach(current, event)
	case EventOTDRScan:
		// A locating scan with no preceding alarm opens its own review case.
		current := e.open(event)
		return e.attach(current, event)
	default:
		// Telemetry and repair actions corroborate an active case only.
		return Outcome{}, ErrNoActiveCase
	}
}

func (e *Engine) open(event Event) *Case {
	at := event.ObservedAt
	opened := &Case{
		ID: e.ids.New("case"), SpanID: event.SpanID, Generation: event.Generation,
		Status: CaseOpen, FirstSeenAt: at, LastEventAt: at,
		Position: Position{Status: PositionInferred, UpdatedAt: at,
			Reason: "awaiting multi-source evidence"},
	}
	e.cases[opened.ID] = opened
	e.openCase[caseKey(event.SpanID, event.Generation)] = opened.ID
	return opened
}

// attach must run under e.mu and assumes current is the open case.
func (e *Engine) attach(current *Case, event Event) (Outcome, error) {
	if current.Status == CaseResolved {
		return Outcome{}, ErrCaseResolved
	}
	if event.DedupKey != "" && current.hasRepeat(event) {
		current.bumpRepeat(event)
		current.LastEventAt = maxTime(current.LastEventAt, event.ObservedAt)
		snapshot := current.Snapshot()
		return Outcome{Case: &snapshot, Duplicate: true}, nil
	}

	current.Events = append(current.Events, event)
	if event.DedupKey != "" {
		current.Repeats = append(current.Repeats, Repeat{
			Type: event.Type, SourceID: event.SourceID, DedupKey: event.DedupKey,
			Count: 1, FirstObservedAt: event.ObservedAt, LastObservedAt: event.ObservedAt,
		})
	}
	current.LastEventAt = maxTime(current.LastEventAt, event.ObservedAt)

	switch event.Type {
	case EventRepeaterAlarm:
		e.applyAlarmPosition(current, event)
	case EventOpticalPower:
		if event.Severity == SeverityRecovered {
			current.OpticsRecovered = true
		}
	case EventOTDRScan:
		e.applyOTDRPosition(current)
	case EventRepairAction:
		switch event.Action {
		case ActionFieldVerify:
			current.Position = Position{
				Status: PositionConfirmed, DistanceKM: event.DistanceKM,
				FieldVerified: true, UpdatedAt: event.ObservedAt,
				Reason: fmt.Sprintf("fault position verified on site by %s", event.SourceID),
			}
		case ActionRepairComplete:
			// Completion means a crew located and treated the fault on site,
			// which is itself a field-verified confirmation.
			if !current.Position.FieldVerified {
				distance := current.Position.DistanceKM
				if event.DistanceKM > 0 {
					distance = event.DistanceKM
				}
				current.Position = Position{
					Status: PositionConfirmed, DistanceKM: distance, FieldVerified: true,
					UpdatedAt: event.ObservedAt,
					Reason:    fmt.Sprintf("position treated on site by %s", event.SourceID),
				}
			}
			e.closeLocked(current, "repair completed: "+event.Detail, event.ObservedAt)
		case ActionFieldSplice:
			// Work progress; the confirmed position is unchanged.
		default:
			// Unknown repair actions are preserved as evidence but do not
			// drive the state machine.
		}
	}

	snapshot := current.Snapshot()
	return Outcome{Case: &snapshot, Attached: true}, nil
}

func (e *Engine) applyAlarmPosition(current *Case, event Event) {
	if current.Position.Status != PositionInferred {
		return
	}
	if event.DistanceKM > 0 {
		current.Position = Position{
			Status: PositionInferred, DistanceKM: event.DistanceKM,
			UpdatedAt: event.ObservedAt,
			Reason:    fmt.Sprintf("inferred from %s alarm direction", event.SourceID),
		}
		return
	}
	current.Position.UpdatedAt = event.ObservedAt
	current.Position.Reason = "inferred from repeater alarm; awaiting locating evidence"
}

// applyOTDRPosition upgrades the position assessment from the OTDR evidence
// collected from both landing stations.
func (e *Engine) applyOTDRPosition(current *Case) {
	latest := make(map[string]Event)
	for _, event := range current.Events {
		if event.Type != EventOTDRScan || event.SourceID == "" {
			continue
		}
		previous, exists := latest[event.SourceID]
		if !exists || event.ObservedAt.After(previous.ObservedAt) {
			latest[event.SourceID] = event
		}
	}
	// A field-verified position is authoritative and never downgraded.
	if current.Position.FieldVerified {
		return
	}
	last := current.Events[len(current.Events)-1]
	if len(latest) < 2 {
		current.Position = Position{
			Status: PositionFieldReview, DistanceKM: last.DistanceKM,
			Confidence: last.Confidence, UpdatedAt: last.ObservedAt,
			Reason: fmt.Sprintf("single-sided OTDR from %s; awaiting peer station or field check", last.SourceID),
		}
		return
	}
	var readings []Event
	for _, event := range latest {
		readings = append(readings, event)
	}
	first, second := readings[0], readings[1]
	agreed := absDiff(first.DistanceKM, second.DistanceKM) <= pairAgreementKM
	distance := first.DistanceKM
	if second.ObservedAt.After(first.ObservedAt) {
		distance = second.DistanceKM
	}
	if agreed {
		current.Position = Position{
			Status: PositionConfirmed, DistanceKM: distance,
			Confidence: highestConfidence(first, second), UpdatedAt: last.ObservedAt,
			Reason: fmt.Sprintf("OTDR readings from %s and %s agree within %.1f km",
				first.SourceID, second.SourceID, pairAgreementKM),
		}
		return
	}
	current.Position = Position{
		Status: PositionFieldReview, DistanceKM: distance,
		Confidence: highestConfidence(first, second), UpdatedAt: last.ObservedAt,
		Reason: fmt.Sprintf("OTDR readings from %s and %s disagree; field review required",
			first.SourceID, second.SourceID),
	}
}

// Resolve closes a case directly. A case may only be closed once its fault
// position has been confirmed; the full evidence chain is retained.
func (e *Engine) Resolve(caseID, resolution string) (Case, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	current := e.cases[caseID]
	if current == nil {
		return Case{}, ErrCaseNotFound
	}
	if current.Status == CaseResolved {
		return Case{}, ErrCaseResolved
	}
	if current.Position.Status != PositionConfirmed {
		return Case{}, fmt.Errorf("%w: case %s position is %s", ErrPositionNotConfirmed, caseID, current.Position.Status)
	}
	e.closeLocked(current, resolution, e.now().UTC())
	return current.Snapshot(), nil
}

func (e *Engine) closeLocked(current *Case, resolution string, at time.Time) {
	at = at.UTC()
	current.Status = CaseResolved
	current.ResolvedAt = at
	current.LastEventAt = maxTime(current.LastEventAt, at)
	if resolution == "" {
		resolution = "repair completed"
	}
	current.Resolution = resolution
	delete(e.openCase, caseKey(current.SpanID, current.Generation))
	e.lastClosed[caseKey(current.SpanID, current.Generation)] = current.ID
}

// GuardClose reports why a case cannot be closed yet.
func (e *Engine) GuardClose(caseID string) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	current := e.cases[caseID]
	if current == nil {
		return ErrCaseNotFound
	}
	if current.Status == CaseResolved {
		return ErrCaseResolved
	}
	if current.Position.Status != PositionConfirmed {
		return fmt.Errorf("%w: case %s position is %s", ErrPositionNotConfirmed, caseID, current.Position.Status)
	}
	return nil
}

// Get returns a deep copy of a case.
func (e *Engine) Get(caseID string) (Case, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	current, exists := e.cases[caseID]
	if !exists {
		return Case{}, false
	}
	return current.Snapshot(), true
}

// OpenFor returns the open case for a span generation, if any.
func (e *Engine) OpenFor(spanID string, generation uint64) (Case, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	id := e.openCase[caseKey(spanID, generation)]
	if id == "" {
		return Case{}, false
	}
	return e.cases[id].Snapshot(), true
}

// List returns all cases ordered by first-seen time.
func (e *Engine) List() []Case {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]Case, 0, len(e.cases))
	for _, current := range e.cases {
		result = append(result, current.Snapshot())
	}
	// Insertion order is map-randomised; order by first-seen time and then
	// by ID for deterministic output on ties.
	for i := 1; i < len(result); i++ {
		for j := i; j > 0; j-- {
			if result[j-1].FirstSeenAt.Before(result[j].FirstSeenAt) ||
				(result[j-1].FirstSeenAt.Equal(result[j].FirstSeenAt) && result[j-1].ID <= result[j].ID) {
				break
			}
			result[j-1], result[j] = result[j], result[j-1]
		}
	}
	return result
}

func (c *Case) hasRepeat(event Event) bool {
	for _, repeat := range c.Repeats {
		if repeat.Type == event.Type && repeat.SourceID == event.SourceID && repeat.DedupKey == event.DedupKey {
			return true
		}
	}
	return false
}

func (c *Case) bumpRepeat(event Event) {
	for index := range c.Repeats {
		repeat := &c.Repeats[index]
		if repeat.Type == event.Type && repeat.SourceID == event.SourceID && repeat.DedupKey == event.DedupKey {
			repeat.Count++
			repeat.LastObservedAt = event.ObservedAt
			return
		}
	}
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func absDiff(a, b float64) float64 {
	if a > b {
		return a - b
	}
	return b - a
}

func highestConfidence(a, b Event) float64 {
	if a.Confidence > b.Confidence {
		return a.Confidence
	}
	return b.Confidence
}
