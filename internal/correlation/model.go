// Package correlation aggregates multi-source submarine-cable events — repeater
// alarms, optical-power telemetry, OTDR scans and repair actions — into a single
// fault case per line (span), topology generation and correlation window.
package correlation

import (
	"errors"
	"time"
)

type EventType string

const (
	// EventRepeaterAlarm is an alarm raised by a submerged repeater or line
	// supervision equipment (LOS, high BER, latching trip, ...).
	EventRepeaterAlarm EventType = "repeater-alarm"
	// EventOpticalPower is an optical-power telemetry reading. Telemetry only
	// corroborates an existing case; it never opens one on its own.
	EventOpticalPower EventType = "optical-power"
	// EventOTDRScan carries a located OTDR reflection from a landing station.
	EventOTDRScan EventType = "otdr-scan"
	// EventRepairAction records a field/repair activity such as a splice, a
	// field position verification or repair completion.
	EventRepairAction EventType = "repair-action"
)

// Repair actions carried by EventRepairAction events.
const (
	ActionFieldSplice    = "splice"
	ActionFieldVerify    = "field-verify"
	ActionRepairComplete = "repair-complete"
)

// Severity markers. SeverityRecovered on an optical-power event marks the
// return of normal optical service.
const (
	SeverityAlarm     = "alarm"
	SeverityDegraded  = "degraded"
	SeverityRecovered = "recovered"
)

// PositionStatus is how firmly the fault position is known.
type PositionStatus string

const (
	// PositionInferred: position derived from indirect evidence (alarm
	// direction, power decay) without a locating measurement.
	PositionInferred PositionStatus = "inferred"
	// PositionConfirmed: position fixed by coherent OTDR evidence or by an
	// on-site verification.
	PositionConfirmed PositionStatus = "confirmed"
	// PositionFieldReview: evidence is single-sided, weak or contradictory;
	// a field crew must inspect before excavation/splice.
	PositionFieldReview PositionStatus = "field-review"
)

type CaseStatus string

const (
	CaseOpen     CaseStatus = "open"
	CaseResolved CaseStatus = "resolved"
)

// Sentinel errors returned by the engine.
var (
	ErrCaseNotFound = errors.New("fault case not found")
	ErrCaseResolved = errors.New("fault case is resolved and its evidence chain is sealed")
	// ErrNoActiveCase is returned when corroborating telemetry arrives while
	// no open case exists for the span generation. Callers treat this as
	// "ignored", not a failure.
	ErrNoActiveCase = errors.New("no open fault case for span generation")
	// ErrPositionNotConfirmed is returned when a case is closed while its
	// position is still inferred or awaiting field review.
	ErrPositionNotConfirmed = errors.New("fault position is not confirmed; field review required")
)

// Event is one entry in the evidence chain.
type Event struct {
	ID         string    `json:"id"`
	Type       EventType `json:"type"`
	SpanID     string    `json:"span_id"`
	Generation uint64    `json:"generation"`
	// SourceID identifies the origin asset: repeater ID, landing station ID
	// or field crew/operation identifier.
	SourceID string `json:"source_id"`
	// RefID references the upstream record (alarm ID, sample ID, OTDR trace
	// ID, acceptance/operation ID) so the evidence chain stays traceable.
	RefID string `json:"ref_id"`
	// Action is meaningful for repair-action events.
	Action string `json:"action,omitempty"`
	// Severity marks alarm/degraded/recovered readings.
	Severity string `json:"severity,omitempty"`
	// DedupKey makes re-notifications of the same underlying condition
	// collapse into one repeat counter instead of new evidence entries.
	DedupKey string `json:"dedup_key,omitempty"`
	// DistanceKM is the fault distance claimed by locating events (OTDR,
	// field verification) or pre-computed by alarm inference.
	DistanceKM float64 `json:"distance_km,omitempty"`
	// Confidence is carried by OTDR locating events, range 0..1.
	Confidence float64 `json:"confidence,omitempty"`
	Detail     string  `json:"detail,omitempty"`

	ObservedAt time.Time `json:"observed_at"`
	ReceivedAt time.Time `json:"received_at"`
}

func (e Event) Validate() error {
	if e.Type != EventRepeaterAlarm && e.Type != EventOpticalPower &&
		e.Type != EventOTDRScan && e.Type != EventRepairAction {
		return errors.New("event type is invalid")
	}
	if e.SpanID == "" || e.Generation == 0 {
		return errors.New("event span and topology generation are required")
	}
	if e.ObservedAt.IsZero() {
		return errors.New("event observed time is required")
	}
	if e.Type == EventOTDRScan && e.DistanceKM <= 0 {
		return errors.New("OTDR event must carry a located distance")
	}
	if e.Action == ActionFieldVerify && e.DistanceKM < 0 {
		return errors.New("field verification must carry a distance")
	}
	if e.Confidence < 0 || e.Confidence > 1 {
		return errors.New("event confidence must be within 0..1")
	}
	return nil
}

// Repeat summarises suppressed duplicate notifications of one condition.
type Repeat struct {
	Type            EventType `json:"type"`
	SourceID        string    `json:"source_id"`
	DedupKey        string    `json:"dedup_key"`
	Count           int       `json:"count"`
	FirstObservedAt time.Time `json:"first_observed_at"`
	LastObservedAt  time.Time `json:"last_observed_at"`
}

// Position is the current fault-position assessment.
type Position struct {
	Status        PositionStatus `json:"status"`
	DistanceKM    float64        `json:"distance_km"`
	Confidence    float64        `json:"confidence,omitempty"`
	FieldVerified bool           `json:"field_verified"`
	Reason        string         `json:"reason"`
	UpdatedAt     time.Time      `json:"updated_at"`
}

// Case is one correlated fault case with its full evidence chain.
type Case struct {
	ID              string     `json:"id"`
	SpanID          string     `json:"span_id"`
	Generation      uint64     `json:"generation"`
	Status          CaseStatus `json:"status"`
	Position        Position   `json:"position"`
	OpticsRecovered bool       `json:"optics_recovered"`
	Events          []Event    `json:"events"`
	Repeats         []Repeat   `json:"repeats"`
	FirstSeenAt     time.Time  `json:"first_seen_at"`
	LastEventAt     time.Time  `json:"last_event_at"`
	ResolvedAt      time.Time  `json:"resolved_at,omitempty"`
	Resolution      string     `json:"resolution,omitempty"`
}

// Snapshot returns a deep copy safe to hand to other goroutines.
func (c Case) Snapshot() Case {
	clone := c
	clone.Events = append([]Event(nil), c.Events...)
	clone.Repeats = append([]Repeat(nil), c.Repeats...)
	return clone
}
