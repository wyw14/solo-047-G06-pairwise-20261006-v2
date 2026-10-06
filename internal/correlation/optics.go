package correlation

import (
	"context"
	"sync"
	"time"

	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/telemetry"
	"github.com/wyw14/cry-103/internal/topology"
)

// DefaultOpticalAlarmDBm is the receive-power level below which an optical
// channel is considered degraded.
const DefaultOpticalAlarmDBm = -20.0

// TopologyReader resolves the current topology generation of a line.
type TopologyReader interface {
	Snapshot(spanID string) (topology.Snapshot, error)
}

// OpticalProjector turns optical-power telemetry into correlation events.
// Only threshold crossings are projected, so the evidence chain receives one
// "degraded" entry when power drops and one "recovered" entry when it returns,
// rather than one entry per sample. It implements telemetry.ProjectionSink.
type OpticalProjector struct {
	mu        sync.RWMutex
	engine    *Engine
	topology  TopologyReader
	ids       platform.IDGenerator
	now       func() time.Time
	lines     map[string]string
	threshold float64
	// degraded tracks repeaters currently below threshold.
	degraded map[string]bool
}

func NewOpticalProjector(engine *Engine, topologyReader TopologyReader, ids platform.IDGenerator, now func() time.Time, repeaterLines map[string]string, alarmThresholdDBm float64) *OpticalProjector {
	if alarmThresholdDBm == 0 {
		alarmThresholdDBm = DefaultOpticalAlarmDBm
	}
	lines := make(map[string]string, len(repeaterLines))
	for repeaterID, spanID := range repeaterLines {
		lines[repeaterID] = spanID
	}
	return &OpticalProjector{
		engine: engine, topology: topologyReader, ids: ids, now: now,
		lines: lines, threshold: alarmThresholdDBm, degraded: make(map[string]bool),
	}
}

func (p *OpticalProjector) ApplySample(_ context.Context, sample telemetry.Sample) error {
	if p.engine == nil || sample.Kind != telemetry.KindOpticalPower {
		return nil
	}
	p.mu.RLock()
	spanID := p.lines[sample.AssetID]
	p.mu.RUnlock()
	if spanID == "" {
		return nil
	}
	snapshot, err := p.topology.Snapshot(spanID)
	if err != nil {
		return nil
	}

	p.mu.Lock()
	wasDegraded := p.degraded[sample.AssetID]
	isDegraded := sample.Value < p.threshold
	p.degraded[sample.AssetID] = isDegraded
	p.mu.Unlock()

	// Project only on a state crossing.
	if isDegraded == wasDegraded {
		return nil
	}
	severity := SeverityDegraded
	if !isDegraded {
		severity = SeverityRecovered
	}
	observed := sample.ObservedAt
	if observed.IsZero() {
		observed = p.now()
	}
	_, err = p.engine.Observe(Event{
		ID: p.ids.New("optics-event"), Type: EventOpticalPower,
		SpanID: spanID, Generation: snapshot.Generation, SourceID: sample.AssetID,
		RefID: sample.ID, Severity: severity, ObservedAt: observed.UTC(),
	})
	if err == ErrNoActiveCase {
		// Power dips without an alarm do not open a case; ignore silently.
		return nil
	}
	return err
}
