package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-103/internal/correlation"
)

// ingestCorrelationEvent feeds one multi-source event (repeater alarm, optical
// power, OTDR scan or repair operation) into the correlation engine. The engine
// folds it into the matching fault case or opens one.
func (s *Server) ingestCorrelationEvent(writer http.ResponseWriter, request *http.Request) {
	spanID := chi.URLParam(request, "id")
	if _, err := s.services.Span(spanID); err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	var input CorrelationEventRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	generation := input.Generation
	if generation == 0 {
		snapshot, err := s.services.Topology.Snapshot(spanID)
		if err != nil {
			writeError(writer, http.StatusBadRequest, err)
			return
		}
		generation = snapshot.Generation
	}
	observedAt := s.services.Clock.Now().UTC()
	if input.ObservedAt != nil {
		observedAt = input.ObservedAt.UTC()
	}
	event := correlation.Event{
		ID: input.ID, SpanID: spanID, Generation: generation, Source: correlation.Source(input.Source),
		AssetID: input.AssetID, Kind: input.Kind, Severity: input.Severity, DistanceKM: input.DistanceKM,
		Confidence: input.Confidence, Summary: input.Summary, Observed: observedAt,
	}
	if event.ID == "" {
		event.ID = s.services.IDs.New("event")
	}
	result, err := s.services.Correlation.Ingest(event)
	if err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	if err := s.services.Persist("correlation-event", event.ID, result); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	status := http.StatusOK
	if result.Created {
		status = http.StatusCreated
	}
	writeJSON(writer, status, result)
}

func (s *Server) listCorrelationCases(writer http.ResponseWriter, request *http.Request) {
	spanID := chi.URLParam(request, "id")
	state := correlation.CaseState(request.URL.Query().Get("state"))
	writeJSON(writer, http.StatusOK, map[string]any{"items": s.services.Correlation.List(spanID, state)})
}

func (s *Server) getCorrelationCase(writer http.ResponseWriter, request *http.Request) {
	c, exists := s.services.Correlation.Get(chi.URLParam(request, "caseID"))
	if !exists {
		writeJSON(writer, http.StatusNotFound, ErrorResponse{Error: "correlation case not found"})
		return
	}
	writeJSON(writer, http.StatusOK, c)
}

func (s *Server) requestCaseReview(writer http.ResponseWriter, request *http.Request) {
	var input CaseReviewRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	c, err := s.services.Correlation.RequestFieldReview(chi.URLParam(request, "caseID"), input.Reason)
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	s.persistCase(writer, c, "correlation-case-review")
}

func (s *Server) confirmCaseLocation(writer http.ResponseWriter, request *http.Request) {
	var input CaseConfirmRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	c, err := s.services.Correlation.ConfirmLocation(chi.URLParam(request, "caseID"), input.DistanceKM, input.Note)
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	s.persistCase(writer, c, "correlation-case-confirm")
}

func (s *Server) closeCorrelationCase(writer http.ResponseWriter, request *http.Request) {
	var input CaseCloseRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	c, err := s.services.Correlation.Close(chi.URLParam(request, "caseID"), input.Note)
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	s.persistCase(writer, c, "correlation-case-close")
}

// persistCase writes the post-action case snapshot (with its full evidence
// chain) to the journal and renders it. A journal failure aborts the response.
func (s *Server) persistCase(writer http.ResponseWriter, c correlation.Snapshot, kind string) {
	if err := s.services.Persist(kind, c.ID, c); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusOK, c)
}
