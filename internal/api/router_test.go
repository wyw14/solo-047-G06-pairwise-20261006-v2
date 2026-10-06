package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/correlation"
	"github.com/wyw14/cry-103/internal/feed"
	"github.com/wyw14/cry-103/internal/incident"
	"github.com/wyw14/cry-103/internal/landing"
	"github.com/wyw14/cry-103/internal/otdr"
	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/repair"
	"github.com/wyw14/cry-103/internal/repeater"
	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/span"
	"github.com/wyw14/cry-103/internal/store"
	"github.com/wyw14/cry-103/internal/switchover"
	"github.com/wyw14/cry-103/internal/telemetry"
	"github.com/wyw14/cry-103/internal/topology"
)

func testServer(t *testing.T) *Server {
	t.Helper()
	clock := fixedClock{now: time.Date(2026, 8, 24, 3, 0, 0, 0, time.UTC)}
	ids := platform.UUIDGenerator{}
	registry := topology.NewRegistry()
	if _, err := registry.Register("S3", topology.PathPrimary, clock.Now()); err != nil {
		t.Fatal(err)
	}
	target, err := span.New("S3", "S3", 126, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	path, err := route.New("R-S3", "S3", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	drainer := route.NewDrainer(0)
	activator := route.NewActivator(registry)
	receipts := landing.NewReceiptRegistry()
	permitService := repair.NewPermitService(feed.NewIsolationService(), receipts, ids, clock.Now)
	circuit, err := feed.NewCircuit("B", "east", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	permitService.RegisterCircuit(circuit)
	repeaterAsset, err := repeater.New("R07", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	lifecycle := incident.NewRepeaterLifecycle()
	projector := repeater.NewHealthProjector(lifecycle, 85)
	projector.Register(repeaterAsset)
	repository := telemetry.NewRepository()
	correlationEngine, err := correlation.NewEngine(ids, clock.Now, correlation.Config{
		Window: 5 * time.Minute, OTDRToleranceKM: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	acceptance := repair.NewAcceptanceService(switchover.NewRestorer(drainer, activator, registry, ids, clock.Now), clock.Now)
	acceptance.Register("S3", target, path)
	services := &Services{
		Clock: clock, IDs: ids, Journal: store.NewJournal(filepath.Join(t.TempDir(), "events.jsonl")),
		Spans: map[string]*span.Span{"S3": target}, Routes: map[string]*route.Route{"R-S3": path},
		Repeaters: map[string]*repeater.Repeater{"R07": repeaterAsset}, Topology: registry,
		OTDR:        otdr.NewSessionService(registry, landing.NewScanner(registry, 0), ids, clock.Now),
		Switchovers: switchover.NewSessionService(drainer, activator, ids, clock.Now, time.Second),
		Permits:     permitService, Receipts: receipts, Acceptance: acceptance,
		Telemetry: telemetry.NewIngestor(repository, clock.Now, projector), TelemetryRepository: repository,
		Admission: incident.NewAdmissionService(telemetry.NewMuteManager(clock.Now)), Correlation: correlationEngine,
	}
	return NewServer(services)
}

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time {
	return c.now
}

func TestHealthAndScanHTTPFlow(t *testing.T) {
	server := testServer(t)
	health := httptest.NewRecorder()
	server.Handler().ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("health status=%d body=%s", health.Code, health.Body.String())
	}

	body, _ := json.Marshal(ScanRequest{StationID: "east", LengthKM: 126, Chunks: 3})
	scan := httptest.NewRecorder()
	server.Handler().ServeHTTP(scan, httptest.NewRequest(http.MethodPost, "/api/spans/S3/otdr-scans", bytes.NewReader(body)))
	if scan.Code != http.StatusCreated {
		t.Fatalf("scan status=%d body=%s", scan.Code, scan.Body.String())
	}
	var trace otdr.Trace
	if err := json.Unmarshal(scan.Body.Bytes(), &trace); err != nil {
		t.Fatal(err)
	}
	if trace.SpanID != "S3" || len(trace.Chunks) != 3 {
		t.Fatalf("unexpected trace: %+v", trace)
	}

	status := httptest.NewRecorder()
	server.Handler().ServeHTTP(status, httptest.NewRequest(http.MethodGet, "/api/system/status", nil))
	if status.Code != http.StatusOK {
		t.Fatalf("status code=%d body=%s", status.Code, status.Body.String())
	}
}

func TestCorrelationCaseHTTPFlow(t *testing.T) {
	server := testServer(t)

	post := func(path string, body any) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(body)
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw)))
		return recorder
	}
	get := func(path string) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		return recorder
	}

	alarmBody := map[string]any{"id": "alarm-1", "generation": 1, "source": "repeater-alarm", "asset_id": "R07", "kind": "latched-trip", "severity": 3}
	first := post("/api/spans/S3/correlation-events", alarmBody)
	if first.Code != http.StatusCreated {
		t.Fatalf("first event creates a case: status=%d body=%s", first.Code, first.Body.String())
	}
	var opened correlation.Result
	if err := json.Unmarshal(first.Body.Bytes(), &opened); err != nil {
		t.Fatal(err)
	}
	if opened.Case.Location.Status != correlation.LocationInferred {
		t.Fatalf("alarm-only case is inferred, got %s", opened.Case.Location.Status)
	}
	caseID := opened.Case.ID

	// Redelivering the identical event id is idempotent.
	dup := post("/api/spans/S3/correlation-events", alarmBody)
	if dup.Code != http.StatusOK {
		t.Fatalf("duplicate event returns 200, got %d body=%s", dup.Code, dup.Body.String())
	}
	var dupResult correlation.Result
	if err := json.Unmarshal(dup.Body.Bytes(), &dupResult); err != nil || !dupResult.Duplicate {
		t.Fatalf("expected duplicate result, body=%s", dup.Body.String())
	}

	distance := 42.0
	otdr := post("/api/spans/S3/correlation-events", map[string]any{
		"id": "scan-1", "generation": 1, "source": "otdr-scan", "asset_id": "east",
		"kind": "otdr-fix", "severity": 4, "distance_km": distance, "confidence": 0.95,
	})
	if otdr.Code != http.StatusOK {
		t.Fatalf("OTDR event joins case: status=%d body=%s", otdr.Code, otdr.Body.String())
	}
	var confirmed correlation.Result
	if err := json.Unmarshal(otdr.Body.Bytes(), &confirmed); err != nil {
		t.Fatal(err)
	}
	if confirmed.Case.ID != caseID || confirmed.Case.Location.Status != correlation.LocationConfirmed {
		t.Fatalf("OTDR must confirm the same case: id=%s loc=%s", confirmed.Case.ID, confirmed.Case.Location.Status)
	}

	closed := post("/api/fault-cases/"+caseID+"/close", map[string]any{"note": "restored"})
	if closed.Code != http.StatusOK {
		t.Fatalf("close status=%d body=%s", closed.Code, closed.Body.String())
	}

	// The closed case still exposes its complete evidence chain.
	fetched := get("/api/fault-cases/" + caseID)
	if fetched.Code != http.StatusOK {
		t.Fatalf("closed case must stay retrievable: %d", fetched.Code)
	}
	var stored correlation.Snapshot
	if err := json.Unmarshal(fetched.Body.Bytes(), &stored); err != nil {
		t.Fatal(err)
	}
	if stored.State != correlation.StateClosed || stored.EvidenceCount != 2 {
		t.Fatalf("closed case retains full chain: state=%s evidence=%d", stored.State, stored.EvidenceCount)
	}
	if len(stored.Evidence) != 2 || stored.Evidence[0].EventID != "alarm-1" || stored.Evidence[1].EventID != "scan-1" {
		t.Fatalf("evidence chain order/content wrong: %+v", stored.Evidence)
	}

	// Only one case exists for the line despite the duplicate delivery.
	listing := get("/api/spans/S3/fault-cases")
	if listing.Code != http.StatusOK {
		t.Fatalf("list status=%d", listing.Code)
	}
	var list struct {
		Items []correlation.Snapshot `json:"items"`
	}
	if err := json.Unmarshal(listing.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("duplicate alarm must not create a parallel case, got %d", len(list.Items))
	}
}
