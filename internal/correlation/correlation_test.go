package correlation

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

type seqIDs struct{ n int }

func (s *seqIDs) New(prefix string) string {
	s.n++
	return fmt.Sprintf("%s-%d", prefix, s.n)
}

func newTestEngine(t *testing.T, at time.Time) (*Engine, *time.Time) {
	t.Helper()
	clock := at
	engine, err := NewEngine(&seqIDs{}, func() time.Time { return clock }, Config{Window: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return engine, &clock
}

func TestAggregatesMultiSourceEvidenceIntoOneCase(t *testing.T) {
	base := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	engine, clock := newTestEngine(t, base)

	alarm := Event{
		Type: EventRepeaterAlarm, SpanID: "S3", Generation: 7, SourceID: "R07",
		RefID: "alarm-1", Severity: SeverityAlarm, DedupKey: "R07:LOS",
		ObservedAt: *clock,
	}
	outcome, err := engine.Observe(alarm)
	if err != nil {
		t.Fatal(err)
	}
	if !outcome.Attached || outcome.Case == nil {
		t.Fatalf("alarm should open a case: %+v", outcome)
	}
	if outcome.Case.Position.Status != PositionInferred {
		t.Fatalf("new case position = %s, want inferred", outcome.Case.Position.Status)
	}
	caseID := outcome.Case.ID

	*clock = clock.Add(10 * time.Second)
	outcome, err = engine.Observe(Event{
		Type: EventOpticalPower, SpanID: "S3", Generation: 7, SourceID: "R07",
		RefID: "sample-9", Severity: SeverityDegraded, ObservedAt: *clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !outcome.Attached || outcome.Case.ID != caseID {
		t.Fatalf("telemetry must join the open case: %+v", outcome)
	}

	*clock = clock.Add(10 * time.Second)
	outcome, err = engine.Observe(Event{
		Type: EventOTDRScan, SpanID: "S3", Generation: 7, SourceID: "east",
		RefID: "scan-east", DistanceKM: 42.0, Confidence: 0.7, ObservedAt: *clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Case.Position.Status != PositionFieldReview {
		t.Fatalf("single-sided OTDR position = %s, want field-review", outcome.Case.Position.Status)
	}

	*clock = clock.Add(10 * time.Second)
	outcome, err = engine.Observe(Event{
		Type: EventOTDRScan, SpanID: "S3", Generation: 7, SourceID: "west",
		RefID: "scan-west", DistanceKM: 42.6, Confidence: 0.8, ObservedAt: *clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Case.Position.Status != PositionConfirmed || outcome.Case.Position.DistanceKM != 42.6 {
		t.Fatalf("agreed OTDR pair should confirm position, got %+v", outcome.Case.Position)
	}

	*clock = clock.Add(time.Hour)
	outcome, err = engine.Observe(Event{
		Type: EventRepairAction, SpanID: "S3", Generation: 7, SourceID: "crew-3",
		Action: ActionRepairComplete, Detail: "splice replaced", ObservedAt: *clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	resolved := outcome.Case
	if resolved.Status != CaseResolved || resolved.ResolvedAt.IsZero() {
		t.Fatalf("repair completion must resolve the case: %+v", resolved)
	}
	if len(resolved.Events) != 5 {
		t.Fatalf("evidence chain = %d events, want 5", len(resolved.Events))
	}
	if resolved.Events[0].RefID != "alarm-1" || resolved.Events[4].Action != ActionRepairComplete {
		t.Fatalf("evidence chain order altered: %+v", resolved.Events)
	}
	if len(engine.List()) != 1 {
		t.Fatalf("exactly one case expected, got %d", len(engine.List()))
	}
}

func TestDuplicateAlarmsNeverOpenParallelCases(t *testing.T) {
	base := time.Date(2026, 10, 5, 9, 0, 0, 0, time.UTC)
	engine, clock := newTestEngine(t, base)

	first, err := engine.Observe(Event{
		Type: EventRepeaterAlarm, SpanID: "S3", Generation: 7, SourceID: "R07",
		Severity: SeverityAlarm, DedupKey: "R07:LOS", ObservedAt: *clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	*clock = clock.Add(30 * time.Second)
	repeat, err := engine.Observe(Event{
		Type: EventRepeaterAlarm, SpanID: "S3", Generation: 7, SourceID: "R07",
		Severity: SeverityAlarm, DedupKey: "R07:LOS", RefID: "alarm-2", ObservedAt: *clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !repeat.Duplicate || repeat.Case.ID != first.Case.ID {
		t.Fatalf("repeat alarm must attach as duplicate: %+v", repeat)
	}
	if len(repeat.Case.Events) != 1 || repeat.Case.Repeats[0].Count != 2 {
		t.Fatalf("repeat must not add evidence, count must be 2: %+v", repeat.Case)
	}

	// A different alarm kind on the same line/generation still joins the
	// same case instead of opening a parallel one.
	*clock = clock.Add(5 * time.Second)
	other, err := engine.Observe(Event{
		Type: EventRepeaterAlarm, SpanID: "S3", Generation: 7, SourceID: "R09",
		Severity: SeverityAlarm, DedupKey: "R09:HIGH-BER", ObservedAt: *clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !other.Attached || other.Case.ID != first.Case.ID {
		t.Fatalf("second source alarm must join the open case: %+v", other)
	}
	if len(engine.List()) != 1 {
		t.Fatalf("one line/generation must yield one case, got %d", len(engine.List()))
	}
}

func TestResolvedCaseSuppressesDuplicatesInsideWindow(t *testing.T) {
	base := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	engine, clock := newTestEngine(t, base)

	opened, err := engine.Observe(Event{
		Type: EventRepeaterAlarm, SpanID: "S3", Generation: 7, SourceID: "R07",
		DedupKey: "R07:LOS", ObservedAt: *clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	*clock = clock.Add(time.Minute)
	if _, err := engine.Observe(Event{
		Type: EventOTDRScan, SpanID: "S3", Generation: 7, SourceID: "east",
		DistanceKM: 42, Confidence: 0.9, ObservedAt: *clock,
	}); err != nil {
		t.Fatal(err)
	}
	*clock = clock.Add(time.Minute)
	if _, err := engine.Observe(Event{
		Type: EventOTDRScan, SpanID: "S3", Generation: 7, SourceID: "west",
		DistanceKM: 42.4, Confidence: 0.9, ObservedAt: *clock,
	}); err != nil {
		t.Fatal(err)
	}
	*clock = clock.Add(time.Minute)
	resolved, err := engine.Resolve(opened.Case.ID, "splice restored")
	if err != nil {
		t.Fatal(err)
	}

	*clock = clock.Add(30 * time.Second)
	duplicate, err := engine.Observe(Event{
		Type: EventRepeaterAlarm, SpanID: "S3", Generation: 7, SourceID: "R07",
		DedupKey: "R07:LOS", ObservedAt: *clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate.Suppressed || duplicate.Case.ID != resolved.ID || len(engine.List()) != 1 {
		t.Fatalf("in-window repeat must be suppressed onto the resolved case: %+v", duplicate)
	}

	*clock = clock.Add(31 * time.Second)
	fresh, err := engine.Observe(Event{
		Type: EventRepeaterAlarm, SpanID: "S3", Generation: 7, SourceID: "R07",
		DedupKey: "R07:LOS", ObservedAt: *clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !fresh.Attached || fresh.Case.ID == resolved.ID {
		t.Fatalf("repeat after the window must open a new case: %+v", fresh)
	}
}

func TestGenerationsAreKeptInSeparateCases(t *testing.T) {
	base := time.Date(2026, 10, 5, 11, 0, 0, 0, time.UTC)
	engine, clock := newTestEngine(t, base)

	genOne, err := engine.Observe(Event{
		Type: EventRepeaterAlarm, SpanID: "S3", Generation: 7, SourceID: "R07",
		DedupKey: "R07:LOS", ObservedAt: *clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	genTwo, err := engine.Observe(Event{
		Type: EventRepeaterAlarm, SpanID: "S3", Generation: 8, SourceID: "R07",
		DedupKey: "R07:LOS", ObservedAt: *clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if genOne.Case.ID == genTwo.Case.ID {
		t.Fatal("topology generations must never be merged into one case")
	}
	if open, ok := engine.OpenFor("S3", 7); !ok || open.ID != genOne.Case.ID {
		t.Fatal("OpenFor must return the generation 7 case")
	}
	if open, ok := engine.OpenFor("S3", 9); ok {
		t.Fatalf("generation 9 must have no open case, got %s", open.ID)
	}
}

func TestPositionLifecycleInferredReviewConfirmed(t *testing.T) {
	base := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	engine, clock := newTestEngine(t, base)

	opened, err := engine.Observe(Event{
		Type: EventRepeaterAlarm, SpanID: "S3", Generation: 7, SourceID: "R07",
		DedupKey: "R07:LOS", ObservedAt: *clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.GuardClose(opened.Case.ID); err == nil {
		t.Fatal("closing an inferred case must be rejected")
	}

	*clock = clock.Add(time.Minute)
	if _, err := engine.Observe(Event{
		Type: EventOTDRScan, SpanID: "S3", Generation: 7, SourceID: "east",
		DistanceKM: 42, Confidence: 0.6, ObservedAt: *clock,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Observe(Event{
		Type: EventOTDRScan, SpanID: "S3", Generation: 7, SourceID: "west",
		DistanceKM: 60, Confidence: 0.6, ObservedAt: *clock,
	}); err != nil {
		t.Fatal(err)
	}
	reviewed, _ := engine.Get(opened.Case.ID)
	if reviewed.Position.Status != PositionFieldReview {
		t.Fatalf("disagreeing OTDR pair must require field review, got %s", reviewed.Position.Status)
	}
	if _, err := engine.Resolve(opened.Case.ID, ""); !errors.Is(err, ErrPositionNotConfirmed) {
		t.Fatalf("resolve field-review case must fail with not-confirmed, got %v", err)
	}

	*clock = clock.Add(2 * time.Hour)
	verified, err := engine.Observe(Event{
		Type: EventRepairAction, SpanID: "S3", Generation: 7, SourceID: "crew-3",
		Action: ActionFieldVerify, DistanceKM: 43.2, ObservedAt: *clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if verified.Case.Position.Status != PositionConfirmed || !verified.Case.Position.FieldVerified {
		t.Fatalf("field verification must confirm position: %+v", verified.Case.Position)
	}

	// Further contradictory OTDR must not downgrade a field-verified position.
	*clock = clock.Add(time.Minute)
	if _, err := engine.Observe(Event{
		Type: EventOTDRScan, SpanID: "S3", Generation: 7, SourceID: "west",
		DistanceKM: 61, Confidence: 0.4, ObservedAt: *clock,
	}); err != nil {
		t.Fatal(err)
	}
	after, _ := engine.Get(opened.Case.ID)
	if !after.Position.FieldVerified || after.Position.DistanceKM != 43.2 {
		t.Fatalf("field-verified position must remain authoritative: %+v", after.Position)
	}

	closed, err := engine.Resolve(opened.Case.ID, "verified and repaired")
	if err != nil {
		t.Fatal(err)
	}
	if closed.Status != CaseResolved || len(closed.Events) != 5 {
		t.Fatalf("resolved case must keep the full chain of 5 events: %+v", closed)
	}
}

func TestTelemetryWithoutActiveCaseIsIgnored(t *testing.T) {
	base := time.Date(2026, 10, 5, 13, 0, 0, 0, time.UTC)
	engine, clock := newTestEngine(t, base)

	_, err := engine.Observe(Event{
		Type: EventOpticalPower, SpanID: "S3", Generation: 7, SourceID: "R07",
		Severity: SeverityDegraded, ObservedAt: *clock,
	})
	if !errors.Is(err, ErrNoActiveCase) {
		t.Fatalf("orphan telemetry must return ErrNoActiveCase, got %v", err)
	}
	if _, err := engine.Observe(Event{
		Type: EventRepairAction, SpanID: "S3", Generation: 7, SourceID: "crew-3",
		Action: ActionFieldSplice, ObservedAt: *clock,
	}); !errors.Is(err, ErrNoActiveCase) {
		t.Fatalf("orphan repair action must return ErrNoActiveCase, got %v", err)
	}
	if len(engine.List()) != 0 {
		t.Fatal("corroborating sources must not open cases by themselves")
	}
}

func TestResolvedCaseSuppressesAcceptanceScansInsideWindow(t *testing.T) {
	base := time.Date(2026, 10, 5, 15, 0, 0, 0, time.UTC)
	engine, clock := newTestEngine(t, base)

	opened, err := engine.Observe(Event{
		Type: EventRepeaterAlarm, SpanID: "S3", Generation: 7, SourceID: "R07",
		DedupKey: "R07:LOS", ObservedAt: *clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	*clock = clock.Add(time.Minute)
	for _, station := range []string{"east", "west"} {
		if _, err := engine.Observe(Event{
			Type: EventOTDRScan, SpanID: "S3", Generation: 7, SourceID: station,
			DistanceKM: 42, Confidence: 0.9, ObservedAt: *clock,
		}); err != nil {
			t.Fatal(err)
		}
	}
	*clock = clock.Add(time.Minute)
	if _, err := engine.Resolve(opened.Case.ID, "splice restored"); err != nil {
		t.Fatal(err)
	}

	// Acceptance OTDR after close must not reopen a parallel case.
	*clock = clock.Add(30 * time.Second)
	acceptance, err := engine.Observe(Event{
		Type: EventOTDRScan, SpanID: "S3", Generation: 7, SourceID: "east",
		DistanceKM: 42, Confidence: 0.95, ObservedAt: *clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !acceptance.Suppressed || acceptance.Case.ID != opened.Case.ID {
		t.Fatalf("post-repair scan must suppress onto the resolved case: %+v", acceptance)
	}
	if len(engine.List()) != 1 {
		t.Fatalf("resolved case must remain the only case, got %d", len(engine.List()))
	}
}

func TestResolvedCaseRejectsFurtherEvidence(t *testing.T) {
	base := time.Date(2026, 10, 5, 14, 0, 0, 0, time.UTC)
	engine, clock := newTestEngine(t, base)

	opened, err := engine.Observe(Event{
		Type: EventRepeaterAlarm, SpanID: "S3", Generation: 7, SourceID: "R07",
		DedupKey: "R07:LOS", ObservedAt: *clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	*clock = clock.Add(2 * time.Minute)
	if _, err := engine.Observe(Event{
		Type: EventOTDRScan, SpanID: "S3", Generation: 7, SourceID: "east",
		DistanceKM: 42, Confidence: 0.9, ObservedAt: *clock,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Observe(Event{
		Type: EventOTDRScan, SpanID: "S3", Generation: 7, SourceID: "west",
		DistanceKM: 42.2, Confidence: 0.9, ObservedAt: *clock,
	}); err != nil {
		t.Fatal(err)
	}
	closed, err := engine.Resolve(opened.Case.ID, "restored")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Resolve(opened.Case.ID, "again"); !errors.Is(err, ErrCaseResolved) {
		t.Fatalf("double resolve must fail sealed, got %v", err)
	}
	stored, ok := engine.Get(closed.ID)
	if !ok || stored.Resolution != "restored" || len(stored.Events) != 3 || len(stored.Repeats) != 1 {
		t.Fatalf("sealed case must retain complete evidence chain: %+v", stored)
	}
}
