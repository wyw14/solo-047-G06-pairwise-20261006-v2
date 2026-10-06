package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-103/internal/correlation"
	"github.com/wyw14/cry-103/internal/otdr"
)

func (s *Server) startScan(writer http.ResponseWriter, request *http.Request) {
	spanID := chi.URLParam(request, "id")
	target, err := s.services.Span(spanID)
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	var input ScanRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	if input.StationID == "" {
		input.StationID = "east"
	}
	if input.LengthKM <= 0 {
		input.LengthKM = target.Snapshot().LengthKM
	}
	if input.Chunks <= 0 {
		input.Chunks = 4
	}
	trace, err := s.services.OTDR.Run(request.Context(), spanID, input.StationID, input.LengthKM, input.Chunks)
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	if err := s.services.Persist("otdr-scan", trace.ID, trace); err != nil {
		writeJSON(writer, http.StatusInternalServerError, err)
		return
	}
	s.projectScan(trace)
	writeJSON(writer, http.StatusCreated, trace)
}

// projectScan feeds a completed OTDR scan into the correlation engine.
func (s *Server) projectScan(trace otdr.Trace) {
	if s.services.Correlation == nil {
		return
	}
	stationID := "east"
	if len(trace.Chunks) > 0 && trace.Chunks[0].StationID != "" {
		stationID = trace.Chunks[0].StationID
	}
	_, _ = s.services.Correlation.Observe(correlation.Event{
		ID: s.services.IDs.New("scan-event"), Type: correlation.EventOTDRScan,
		SpanID: trace.SpanID, Generation: trace.Generation, SourceID: stationID,
		RefID: trace.ID, DistanceKM: trace.Location.DistanceKM,
		Confidence: trace.Location.Confidence, ObservedAt: trace.CompletedAt.UTC(),
	})
}

func (s *Server) getSpan(writer http.ResponseWriter, request *http.Request) {
	target, err := s.services.Span(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	writeJSON(writer, http.StatusOK, target.Snapshot())
}
