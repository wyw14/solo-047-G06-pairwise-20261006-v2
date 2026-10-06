// Package correlation aggregates multi-source submarine-cable events — repeater
// alarms, optical-power telemetry, OTDR scans and repair operations — into a
// single fault case per line, topology generation and time window.
package correlation

import (
	"errors"
	"time"
)

// Source identifies where an event originated.
type Source string

const (
	SourceRepeaterAlarm Source = "repeater-alarm"
	SourceOpticalPower  Source = "optical-power"
	SourceOTDRScan      Source = "otdr-scan"
	SourceRepair        Source = "repair"
)

func (s Source) valid() bool {
	switch s {
	case SourceRepeaterAlarm, SourceOpticalPower, SourceOTDRScan, SourceRepair:
		return true
	}
	return false
}

// LocationStatus is how precisely the fault position is known.
type LocationStatus string

const (
	// LocationInferred is a position derived from indirect evidence
	// (repeater alarms, optical-power degradation) and not yet pinned by OTDR.
	LocationInferred LocationStatus = "inferred"
	// LocationConfirmed is pinned by OTDR evidence for the case generation.
	LocationConfirmed LocationStatus = "confirmed"
	// LocationFieldReview means remote evidence disagrees or is incomplete;
	// a field crew must verify the position before repair proceeds.
	LocationFieldReview LocationStatus = "field-review"
)

// CaseState is the lifecycle state of a fault case.
type CaseState string

const (
	StateOpen      CaseState = "open"
	StateReviewing CaseState = "reviewing"
	StateClosed    CaseState = "closed"
)

// Event is one multi-source observation offered to the correlation engine.
type Event struct {
	// ID is the source-system identifier of the observation. It is used for
	// idempotent ingestion: delivering the same event twice never appends
	// duplicate evidence.
	ID string `json:"id"`
	// SpanID is the line (wet span) the event belongs to.
	SpanID string `json:"span_id"`
	// Generation is the topology generation the event was observed under.
	// Events from different generations are never merged into one case.
	Generation uint64 `json:"generation"`
	Source     Source `json:"source"`
	// AssetID is the originating asset, e.g. repeater R07 or landing station.
	AssetID string `json:"asset_id"`
	// Kind is the source-specific event kind, e.g. "latched-trip" or "los".
	Kind string `json:"kind"`
	// Severity ranks urgency; higher values outrank lower ones for the case head.
	Severity int `json:"severity"`
	// DistanceKM is the distance from the reference landing, when known.
	DistanceKM *float64 `json:"distance_km,omitempty"`
	// Confidence in [0,1] accompanies a distance estimate, if any.
	Confidence float64 `json:"confidence,omitempty"`
	// Summary is human-readable detail kept verbatim in the evidence chain.
	Summary  string    `json:"summary,omitempty"`
	Observed time.Time `json:"observed_at"`
}

func (e Event) Validate() error {
	if e.ID == "" {
		return errors.New("event id is required")
	}
	if e.SpanID == "" {
		return errors.New("event span id is required")
	}
	if e.Generation == 0 {
		return errors.New("event topology generation is required")
	}
	if !e.Source.valid() {
		return errors.New("event source is invalid")
	}
	if e.Observed.IsZero() {
		return errors.New("event observed time is required")
	}
	if e.DistanceKM != nil && (*e.DistanceKM < 0 || e.Confidence < 0 || e.Confidence > 1) {
		return errors.New("event distance must be non-negative and confidence within [0,1]")
	}
	return nil
}

// HasDistance reports whether the event carries a usable position estimate.
func (e Event) HasDistance() bool {
	return e.DistanceKM != nil
}

// Evidence is an immutable entry in a case's evidence chain.
type Evidence struct {
	Seq        int       `json:"seq"`
	EventID    string    `json:"event_id"`
	Source     Source    `json:"source"`
	AssetID    string    `json:"asset_id"`
	Kind       string    `json:"kind"`
	Severity   int       `json:"severity"`
	DistanceKM *float64  `json:"distance_km,omitempty"`
	Confidence float64   `json:"confidence,omitempty"`
	Summary    string    `json:"summary,omitempty"`
	AddedAt    time.Time `json:"added_at"`
	Observed   time.Time `json:"observed_at"`
}

// Location is the case's current best understanding of the fault position.
type Location struct {
	Status     LocationStatus `json:"status"`
	DistanceKM *float64       `json:"distance_km,omitempty"`
	// Basis is the event id that the current position rests on.
	Basis    string  `json:"basis,omitempty"`
	Review   string  `json:"review_reason,omitempty"`
	Accuracy float64 `json:"accuracy,omitempty"`
}

// Transition records one case-level status change, preserving the audit trail.
type Transition struct {
	From     CaseState      `json:"from"`
	To       CaseState      `json:"to"`
	Location LocationStatus `json:"location"`
	Reason   string         `json:"reason"`
	At       time.Time      `json:"at"`
}

// Case is one correlated fault. Snapshot is a value type safe to serialize;
// mutation goes through Case methods under the case's own lock.
type Case struct {
	ID         string    `json:"id"`
	SpanID     string    `json:"span_id"`
	Generation uint64    `json:"generation"`
	State      CaseState `json:"state"`
	Severity   int       `json:"severity"`
	Location   Location  `json:"location"`

	OpenedAt  time.Time  `json:"opened_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	ClosedAt  *time.Time `json:"closed_at,omitempty"`
	CloseNote string     `json:"close_note,omitempty"`

	// Window bounds: the case keeps accepting correlated events while their
	// observed time overlaps [FirstObserved, LastObserved]+window.
	FirstObserved time.Time `json:"first_observed_at"`
	LastObserved  time.Time `json:"last_observed_at"`

	// Evidence is the complete, append-only evidence chain. It is frozen on
	// close and returned in full when the case is retrieved.
	Evidence    []Evidence   `json:"evidence"`
	Transitions []Transition `json:"transitions"`
}

// Snapshot view returned by the engine. The evidence chain is copied so the
// caller cannot mutate case internals.
type Snapshot struct {
	Case
	EvidenceCount int `json:"evidence_count"`
}

// Counts groups evidence by source for quick inspection.
func (c *Case) Counts() map[Source]int {
	counts := make(map[Source]int, 4)
	for _, item := range c.Evidence {
		counts[item.Source]++
	}
	return counts
}
