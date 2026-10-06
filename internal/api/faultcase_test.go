package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wyw14/cry-103/internal/correlation"
)

func doFaultRequest(t *testing.T, server *Server, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(encoded)
	} else {
		reader = bytes.NewReader(nil)
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(method, path, reader))
	return recorder
}

func openFaultCaseThroughAPI(t *testing.T, server *Server) correlation.Case {
	t.Helper()
	first := doFaultRequest(t, server, http.MethodPost, "/api/spans/S3/alarms",
		AlarmRequest{Kind: "LOS", SourceID: "R07"})
	if first.Code != http.StatusCreated {
		t.Fatalf("first alarm status=%d body=%s", first.Code, first.Body.String())
	}
	repeat := doFaultRequest(t, server, http.MethodPost, "/api/spans/S3/alarms",
		AlarmRequest{Kind: "LOS", SourceID: "R07"})
	if repeat.Code != http.StatusCreated {
		t.Fatalf("repeat alarm status=%d body=%s", repeat.Code, repeat.Body.String())
	}
	list := doFaultRequest(t, server, http.MethodGet, "/api/fault-cases", nil)
	if list.Code != http.StatusOK {
		t.Fatalf("list cases status=%d", list.Code)
	}
	var payload struct {
		Items []correlation.Case `json:"items"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Items) != 1 {
		t.Fatalf("repeat alarms must not open parallel cases, got %d", len(payload.Items))
	}
	return payload.Items[0]
}

func TestFaultCaseCorrelationHTTPFlow(t *testing.T) {
	server := testServer(t)
	opened := openFaultCaseThroughAPI(t, server)
	if opened.Position.Status != correlation.PositionInferred {
		t.Fatalf("position = %s, want inferred", opened.Position.Status)
	}
	if opened.Repeats[0].Count != 2 {
		t.Fatalf("repeat counter = %d, want 2", opened.Repeats[0].Count)
	}

	// Optical power dip corroborates the open case.
	optics := doFaultRequest(t, server, http.MethodPost, "/api/repeaters/R07/telemetry",
		TelemetryRequest{Kind: "optical_power", Value: -22.4, Unit: "dBm", Sequence: 1, Generation: 1})
	if optics.Code != http.StatusAccepted {
		t.Fatalf("telemetry status=%d body=%s", optics.Code, optics.Body.String())
	}

	// Scans from both landing stations locate the same reflection.
	east := doFaultRequest(t, server, http.MethodPost, "/api/spans/S3/otdr-scans",
		ScanRequest{StationID: "east", LengthKM: 126, Chunks: 4})
	if east.Code != http.StatusCreated {
		t.Fatalf("east scan status=%d body=%s", east.Code, east.Body.String())
	}
	west := doFaultRequest(t, server, http.MethodPost, "/api/spans/S3/otdr-scans",
		ScanRequest{StationID: "west", LengthKM: 126, Chunks: 4})
	if west.Code != http.StatusCreated {
		t.Fatalf("west scan status=%d body=%s", west.Code, west.Body.String())
	}

	confirmed := doFaultRequest(t, server, http.MethodGet, "/api/fault-cases/"+opened.ID, nil)
	var current correlation.Case
	if err := json.Unmarshal(confirmed.Body.Bytes(), &current); err != nil {
		t.Fatal(err)
	}
	if current.Position.Status != correlation.PositionConfirmed {
		t.Fatalf("paired OTDR must confirm position, got %s (%s)", current.Position.Status, current.Position.Reason)
	}
	if len(current.Events) != 4 {
		t.Fatalf("evidence chain = %d events, want alarm+optics+2 scans: %+v", len(current.Events), current.Events)
	}

	// Resolve seals the evidence chain.
	resolved := doFaultRequest(t, server, http.MethodPost,
		"/api/fault-cases/"+opened.ID+"/resolve", ResolveCaseRequest{Resolution: "splice restored"})
	if resolved.Code != http.StatusOK {
		t.Fatalf("resolve status=%d body=%s", resolved.Code, resolved.Body.String())
	}
	var sealed correlation.Case
	if err := json.Unmarshal(resolved.Body.Bytes(), &sealed); err != nil {
		t.Fatal(err)
	}
	if sealed.Status != correlation.CaseResolved || sealed.Resolution != "splice restored" || sealed.ResolvedAt.IsZero() {
		t.Fatalf("case not properly sealed: %+v", sealed)
	}
	if len(sealed.Events) != 4 {
		t.Fatalf("sealed evidence chain must be preserved, got %d events", len(sealed.Events))
	}
	if sealed.Events[0].Type != correlation.EventRepeaterAlarm ||
		sealed.Events[len(sealed.Events)-1].Type != correlation.EventOTDRScan {
		t.Fatal("sealed evidence chain order corrupted")
	}
}

func TestResolveRejectsUnconfirmedCase(t *testing.T) {
	server := testServer(t)
	opened := openFaultCaseThroughAPI(t, server)
	resolved := doFaultRequest(t, server, http.MethodPost,
		"/api/fault-cases/"+opened.ID+"/resolve", ResolveCaseRequest{Resolution: "too early"})
	if resolved.Code != http.StatusConflict {
		t.Fatalf("resolving an inferred case must be 409, got %d body=%s", resolved.Code, resolved.Body.String())
	}
}

func TestFieldReviewThenRepairEventResolves(t *testing.T) {
	server := testServer(t)
	opened := openFaultCaseThroughAPI(t, server)

	// A single-sided scan forces field review.
	scan := doFaultRequest(t, server, http.MethodPost, "/api/spans/S3/otdr-scans",
		ScanRequest{StationID: "east", LengthKM: 126, Chunks: 4})
	if scan.Code != http.StatusCreated {
		t.Fatalf("scan status=%d body=%s", scan.Code, scan.Body.String())
	}
	reviewed := doFaultRequest(t, server, http.MethodGet, "/api/fault-cases/"+opened.ID, nil)
	var current correlation.Case
	if err := json.Unmarshal(reviewed.Body.Bytes(), &current); err != nil {
		t.Fatal(err)
	}
	if current.Position.Status != correlation.PositionFieldReview {
		t.Fatalf("single-sided scan must require field review, got %s", current.Position.Status)
	}

	// Crew reports the verified position then completes the repair.
	verify := doFaultRequest(t, server, http.MethodPost,
		fmt.Sprintf("/api/spans/S3/repair-events"),
		RepairEventRequest{Action: correlation.ActionFieldVerify, SourceID: "crew-3", DistanceKM: 28.4})
	if verify.Code != http.StatusAccepted {
		t.Fatalf("field verify status=%d body=%s", verify.Code, verify.Body.String())
	}
	complete := doFaultRequest(t, server, http.MethodPost,
		"/api/spans/S3/repair-events",
		RepairEventRequest{Action: correlation.ActionRepairComplete, SourceID: "crew-3", DistanceKM: 28.4, Detail: "splice replaced"})
	if complete.Code != http.StatusOK {
		t.Fatalf("repair completion status=%d body=%s", complete.Code, complete.Body.String())
	}
	var outcome correlation.Outcome
	if err := json.Unmarshal(complete.Body.Bytes(), &outcome); err != nil {
		t.Fatal(err)
	}
	if outcome.Case == nil || outcome.Case.Status != correlation.CaseResolved ||
		!outcome.Case.Position.FieldVerified {
		t.Fatalf("repair completion must resolve a field-verified case: %+v", outcome)
	}
}
