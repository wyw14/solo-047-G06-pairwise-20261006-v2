package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-103/internal/correlation"
)

func (s *Server) listFaultCases(writer http.ResponseWriter, request *http.Request) {
	spanID := request.URL.Query().Get("span_id")
	cases := s.services.Correlation.List()
	if spanID != "" {
		filtered := cases[:0]
		for _, current := range cases {
			if current.SpanID == spanID {
				filtered = append(filtered, current)
			}
		}
		cases = filtered
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": cases})
}

func (s *Server) getFaultCase(writer http.ResponseWriter, request *http.Request) {
	current, exists := s.services.Correlation.Get(chi.URLParam(request, "id"))
	if !exists {
		writeJSON(writer, http.StatusNotFound, ErrorResponse{Error: "fault case not found"})
		return
	}
	writeJSON(writer, http.StatusOK, current)
}

func (s *Server) resolveFaultCase(writer http.ResponseWriter, request *http.Request) {
	var input ResolveCaseRequest
	if err := decodeJSON(request, &input); err != nil {
		writeJSON(writer, http.StatusBadRequest, err)
		return
	}
	current, err := s.services.Correlation.Resolve(chi.URLParam(request, "id"), input.Resolution)
	if err != nil {
		writeCorrelationError(writer, err)
		return
	}
	if err := s.services.Persist("fault-case", current.ID, current); err != nil {
		writeJSON(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusOK, current)
}

func (s *Server) recordRepairEvent(writer http.ResponseWriter, request *http.Request) {
	spanID := chi.URLParam(request, "id")
	target, err := s.services.Span(spanID)
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	var input RepairEventRequest
	if err := decodeJSON(request, &input); err != nil {
		writeJSON(writer, http.StatusBadRequest, err)
		return
	}
	generation := input.Generation
	if generation == 0 {
		snapshot, topologyErr := s.services.Topology.Snapshot(spanID)
		if topologyErr != nil {
			writeError(writer, http.StatusConflict, topologyErr)
			return
		}
		generation = snapshot.Generation
	}
	sourceID := input.SourceID
	if sourceID == "" {
		sourceID = "field-crew"
	}
	event := correlation.Event{
		ID: s.services.IDs.New("repair-event"), Type: correlation.EventRepairAction,
		SpanID: spanID, Generation: generation, SourceID: sourceID,
		RefID: target.Snapshot().ID + "/" + input.Action, Action: input.Action,
		DistanceKM: input.DistanceKM, Detail: input.Detail,
		ObservedAt: s.services.Clock.Now().UTC(),
	}
	outcome, err := s.services.Correlation.Observe(event)
	if err != nil {
		writeCorrelationError(writer, err)
		return
	}
	if err := s.services.Persist("repair-event", event.ID, event); err != nil {
		writeJSON(writer, http.StatusInternalServerError, err)
		return
	}
	status := http.StatusAccepted
	if outcome.Case != nil && outcome.Case.Status == correlation.CaseResolved {
		status = http.StatusOK
		if err := s.services.Persist("fault-case", outcome.Case.ID, outcome.Case); err != nil {
			writeJSON(writer, http.StatusInternalServerError, err)
			return
		}
	}
	writeJSON(writer, status, outcome)
}

func writeCorrelationError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, correlation.ErrCaseNotFound):
		writeError(writer, http.StatusNotFound, err)
	case errors.Is(err, correlation.ErrNoActiveCase):
		writeError(writer, http.StatusConflict, err)
	case errors.Is(err, correlation.ErrCaseResolved), errors.Is(err, correlation.ErrPositionNotConfirmed):
		writeError(writer, http.StatusConflict, err)
	default:
		writeError(writer, http.StatusBadRequest, err)
	}
}
