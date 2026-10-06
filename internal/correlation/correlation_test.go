package correlation

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type fakeIDs struct {
	mu sync.Mutex
	n  int
}

func (f *fakeIDs) New(prefix string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	return fmt.Sprintf("%s-%d", prefix, f.n)
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func newTestEngine(t *testing.T) (*Engine, *fakeClock) {
	t.Helper()
	clock := &fakeClock{t: time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)}
	engine, err := NewEngine(&fakeIDs{}, clock.now, Config{Window: 5 * time.Minute, OTDRToleranceKM: 2})
	if err != nil {
		t.Fatal(err)
	}
	return engine, clock
}

func km(v float64) *float64 { return &v }

func repeaterAlarm(id, spanID string, generation uint64, at time.Time) Event {
	return Event{ID: id, SpanID: spanID, Generation: generation, Source: SourceRepeaterAlarm,
		AssetID: "R07", Kind: "latched-trip", Severity: 3, Observed: at}
}

func TestAggregatesMultipleSourcesIntoOneCase(t *testing.T) {
	engine, clock := newTestEngine(t)
	base := clock.t

	alarm := repeaterAlarm("evt-alarm", "S3", 1, base)
	r1, err := engine.Ingest(alarm)
	if err != nil {
		t.Fatal(err)
	}
	if !r1.Created || r1.Case.ID != "case-1" {
		t.Fatalf("first event must open a case: %+v", r1)
	}
	if r1.LocationTo != LocationInferred {
		t.Fatalf("alarm-only evidence is inferred, got %s", r1.LocationTo)
	}

	r2, err := engine.Ingest(Event{
		ID: "evt-power", SpanID: "S3", Generation: 1, Source: SourceOpticalPower,
		AssetID: "east", Kind: "rx-power-low", Severity: 2, DistanceKM: km(42), Confidence: 0.6,
		Observed: base.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if r2.Created || r2.LocationTo != LocationInferred || r2.Case.Location.DistanceKM == nil ||
		*r2.Case.Location.DistanceKM != 42 {
		t.Fatalf("optical power should refine inferred location: %+v", r2.Case.Location)
	}

	r3, err := engine.Ingest(Event{
		ID: "evt-otdr", SpanID: "S3", Generation: 1, Source: SourceOTDRScan,
		AssetID: "east", Kind: "otdr-fix", Severity: 4, DistanceKM: km(42.5), Confidence: 0.95,
		Observed: base.Add(2 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if r3.Created || r3.LocationTo != LocationConfirmed || r3.Case.State != StateOpen {
		t.Fatalf("agreeing OTDR must confirm: %+v state=%s", r3.Case.Location, r3.Case.State)
	}

	r4, err := engine.Ingest(Event{
		ID: "evt-repair", SpanID: "S3", Generation: 1, Source: SourceRepair,
		AssetID: "crew-7", Kind: "splice-began", Severity: 1, Observed: base.Add(3 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if r4.Created || len(r4.Case.Evidence) != 4 {
		t.Fatalf("repair op joins the same case, evidence=%d", len(r4.Case.Evidence))
	}
	if got := r4.Case.Severity; got != 4 {
		t.Fatalf("case severity should track the highest evidence, got %d", got)
	}

	cases := engine.List("", "")
	if len(cases) != 1 {
		t.Fatalf("four related sources produce one case, got %d", len(cases))
	}
	counts := cases[0].Counts()
	for source, want := range map[Source]int{
		SourceRepeaterAlarm: 1, SourceOpticalPower: 1, SourceOTDRScan: 1, SourceRepair: 1,
	} {
		if counts[source] != want {
			t.Fatalf("evidence count for %s = %d, want %d", source, counts[source], want)
		}
	}
}

func TestRepeatedAlarmDoesNotOpenParallelCase(t *testing.T) {
	engine, clock := newTestEngine(t)
	base := clock.t

	alarm := repeaterAlarm("evt-alarm", "S3", 1, base)
	if _, err := engine.Ingest(alarm); err != nil {
		t.Fatal(err)
	}
	// Exact redelivery of the same alarm: deduped, no second evidence entry.
	again, err := engine.Ingest(alarm)
	if err != nil {
		t.Fatal(err)
	}
	if !again.Duplicate || again.Created || again.Appended || again.Case.EvidenceCount != 1 {
		t.Fatalf("repeated alarm must be a duplicate: %+v count=%d", again, again.Case.EvidenceCount)
	}

	// A distinct alarm of the same kind while the window is still open joins
	// the existing case rather than running in parallel.
	next, err := engine.Ingest(repeaterAlarm("evt-alarm-2", "S3", 1, base.Add(30*time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	if next.Created || next.Duplicate || next.Case.EvidenceCount != 2 {
		t.Fatalf("fresh alarm within window joins the case: %+v count=%d", next, next.Case.EvidenceCount)
	}
	if len(engine.List("", "")) != 1 {
		t.Fatal("repeated/related alarms must not produce parallel cases")
	}
}

func TestGenerationsAreNeverMerged(t *testing.T) {
	engine, clock := newTestEngine(t)
	base := clock.t

	r1, err := engine.Ingest(repeaterAlarm("evt-g1", "S3", 1, base))
	if err != nil {
		t.Fatal(err)
	}
	r2, err := engine.Ingest(repeaterAlarm("evt-g2", "S3", 2, base.Add(time.Minute)))
	if err != nil {
		t.Fatal(err)
	}
	if !r2.Created || r2.Case.ID == r1.Case.ID {
		t.Fatal("events from different topology generations must form separate cases")
	}
	if len(engine.List("S3", "")) != 2 {
		t.Fatal("expected one open case per generation")
	}
}

func TestEventOutsideWindowOpensSeparateCase(t *testing.T) {
	engine, clock := newTestEngine(t)
	base := clock.t

	if _, err := engine.Ingest(repeaterAlarm("evt-1", "S3", 1, base)); err != nil {
		t.Fatal(err)
	}
	// Gap exceeds the 5-minute window: a later fault, not the same outage.
	later, err := engine.Ingest(repeaterAlarm("evt-2", "S3", 1, base.Add(6*time.Minute)))
	if err != nil {
		t.Fatal(err)
	}
	if !later.Created {
		t.Fatal("an event beyond the correlation window must open a new case")
	}
}

func TestConflictingOTDRForcesFieldReview(t *testing.T) {
	engine, clock := newTestEngine(t)
	base := clock.t

	if _, err := engine.Ingest(Event{
		ID: "evt-power", SpanID: "S3", Generation: 1, Source: SourceOpticalPower,
		AssetID: "east", Kind: "rx-power-low", DistanceKM: km(42), Confidence: 0.6, Observed: base,
	}); err != nil {
		t.Fatal(err)
	}
	conflict, err := engine.Ingest(Event{
		ID: "evt-otdr", SpanID: "S3", Generation: 1, Source: SourceOTDRScan,
		AssetID: "west", Kind: "otdr-fix", DistanceKM: km(80), Confidence: 0.9, Observed: base.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if conflict.LocationTo != LocationFieldReview || conflict.Case.State != StateReviewing {
		t.Fatalf("contradictory OTDR must force field review: loc=%s state=%s",
			conflict.LocationTo, conflict.Case.State)
	}
	if conflict.Case.Location.Review == "" {
		t.Fatal("field review must explain the disagreement")
	}

	// Further remote OTDR cannot self-resolve a review; the case stays reviewing.
	more, err := engine.Ingest(Event{
		ID: "evt-otdr-2", SpanID: "S3", Generation: 1, Source: SourceOTDRScan,
		AssetID: "east", Kind: "otdr-fix", DistanceKM: km(42.2), Confidence: 0.95, Observed: base.Add(2 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if more.Case.Location.Status != LocationFieldReview {
		t.Fatal("remote evidence must not clear field review without field verification")
	}

	verified, err := engine.ConfirmLocation(more.Case.ID, 42.1, "crew located failed splice")
	if err != nil {
		t.Fatal(err)
	}
	if verified.Location.Status != LocationConfirmed || verified.State != StateOpen {
		t.Fatalf("field verification restores confirmed/open: loc=%s state=%s",
			verified.Location.Status, verified.State)
	}
}

func TestManualFieldReviewRequest(t *testing.T) {
	engine, clock := newTestEngine(t)
	created, err := engine.Ingest(repeaterAlarm("evt-1", "S3", 1, clock.t))
	if err != nil {
		t.Fatal(err)
	}
	review, err := engine.RequestFieldReview(created.Case.ID, "ambiguous two-end OTDR")
	if err != nil {
		t.Fatal(err)
	}
	if review.State != StateReviewing || review.Location.Status != LocationFieldReview {
		t.Fatalf("manual review flags state and location: %+v", review)
	}
}

func TestCloseFreezesCaseAndPreservesEvidenceChain(t *testing.T) {
	engine, clock := newTestEngine(t)
	base := clock.t

	first, err := engine.Ingest(repeaterAlarm("evt-1", "S3", 1, base))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Ingest(Event{
		ID: "evt-otdr", SpanID: "S3", Generation: 1, Source: SourceOTDRScan,
		AssetID: "east", Kind: "otdr-fix", DistanceKM: km(42), Confidence: 0.9, Observed: base.Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}

	closed, err := engine.Close(first.Case.ID, "restored and optically proven")
	if err != nil {
		t.Fatal(err)
	}
	if closed.State != StateClosed || closed.ClosedAt == nil || closed.CloseNote == "" {
		t.Fatalf("close must stamp the case: %+v", closed)
	}

	// Closing again is rejected.
	if _, err := engine.Close(first.Case.ID, "again"); err == nil {
		t.Fatal("a closed case cannot be closed again")
	}
	// Closed cases reject lifecycle actions.
	if _, err := engine.RequestFieldReview(first.Case.ID, "x"); err == nil {
		t.Fatal("a closed case cannot move back to review")
	}

	// A new event inside the time window must NOT attach to the closed case;
	// it opens a fresh case because the old fault is fully resolved.
	after, err := engine.Ingest(repeaterAlarm("evt-2", "S3", 1, base.Add(2*time.Minute)))
	if err != nil {
		t.Fatal(err)
	}
	if !after.Created || after.Case.ID == first.Case.ID {
		t.Fatal("events after close must open a new case, not resurrect the closed one")
	}

	// The closed case retains its complete, ordered evidence chain and audit trail.
	stored, ok := engine.Get(first.Case.ID)
	if !ok {
		t.Fatal("closed case must remain retrievable")
	}
	if stored.EvidenceCount != 2 {
		t.Fatalf("evidence chain must be preserved on close, got %d", stored.EvidenceCount)
	}
	for index, item := range stored.Evidence {
		if item.Seq != index+1 {
			t.Fatalf("evidence sequence broken at %d: %+v", index, item)
		}
	}
	if len(stored.Transitions) == 0 {
		t.Fatal("closed case must keep its transition history")
	}
	last := stored.Transitions[len(stored.Transitions)-1]
	if last.To != StateClosed {
		t.Fatalf("final transition must be the close, got %s", last.To)
	}
}

func TestRejectsInvalidEvent(t *testing.T) {
	engine, _ := newTestEngine(t)
	if _, err := engine.Ingest(Event{ID: "x", SpanID: "S3", Generation: 1, Source: Source("unknown")}); err == nil {
		t.Fatal("an event with an invalid source must be rejected")
	}
	if _, err := engine.Ingest(Event{SpanID: "S3", Generation: 1, Source: SourceRepair}); err == nil {
		t.Fatal("an event without an id must be rejected")
	}
}
