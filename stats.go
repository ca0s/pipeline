package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.uber.org/atomic"
)

var PipelineStatDB = "pipeline_stats_db"

type StatDB[E Traceable] struct {
	itemLock sync.RWMutex
	items    map[Processor[E]]*Stats
}

func NewStatDB[E Traceable]() *StatDB[E] {
	return &StatDB[E]{
		items: make(map[Processor[E]]*Stats),
	}
}

type Stats struct {
	Input       atomic.Int64 `json:"input"`
	Output      atomic.Int64 `json:"output"`
	Passthrough atomic.Int64 `json:"passthrough"`
	Failed      atomic.Int64 `json:"failed"`

	LastInput       time.Time `json:"last_input"`
	LastOutput      time.Time `json:"last_output"`
	LastPassthrough time.Time `json:"last_passthrough"`
	LastFailure     time.Time `json:"last_failure"`

	Started  time.Time `json:"started"`
	Finished time.Time `json:"finished"`

	Name string `json:"name"`
}

func NewStats(name string) *Stats {
	return &Stats{
		Name: name,
	}
}

func (d *StatDB[E]) MarshalJSON() ([]byte, error) {
	d.itemLock.RLock()
	defer d.itemLock.RUnlock()

	data := make(map[string]*Stats)

	for p, stats := range d.items {
		data[fmt.Sprintf("%s/%p", p.Name(), p)] = stats
	}

	return json.Marshal(data)
}

func WithStats[E Traceable](ctx context.Context, sdb *StatDB[E]) context.Context {
	return context.WithValue(ctx, PipelineStatDB, sdb)
}

func TrackStarted[E Traceable](ctx context.Context, processor Processor[E]) {
	statDB, ok := ctx.Value(PipelineStatDB).(*StatDB[E])
	if !ok {
		return
	}

	statDB.trackStarted(processor)
}

func TrackFinished[E Traceable](ctx context.Context, processor Processor[E]) {
	statDB, ok := ctx.Value(PipelineStatDB).(*StatDB[E])
	if !ok {
		return
	}

	statDB.trackFinished(processor)
}

func TrackInput[E Traceable](ctx context.Context, processor Processor[E]) {
	statDB, ok := ctx.Value(PipelineStatDB).(*StatDB[E])
	if !ok {
		return
	}

	statDB.trackInput(processor)
}

func TrackOutput[E Traceable](ctx context.Context, processor Processor[E], obj Traceable) {
	if HasTracesEnabled(ctx) {
		obj.AddTrace(processor.Name())
	}

	statDB, ok := ctx.Value(PipelineStatDB).(*StatDB[E])
	if !ok {
		return
	}

	statDB.trackOutput(processor)
}

func TrackPassthrough[E Traceable](ctx context.Context, processor Processor[E], obj Traceable) {
	statDB, ok := ctx.Value(PipelineStatDB).(*StatDB[E])
	if !ok {
		return
	}

	statDB.trackOutput(processor)
}

func (db *StatDB[E]) getStats(p Processor[E]) *Stats {
	db.itemLock.Lock()
	defer db.itemLock.Unlock()

	stats, ok := db.items[p]
	if !ok {
		stats = NewStats(p.Name())
		db.items[p] = stats
	}

	return stats
}

func (db *StatDB[E]) trackStarted(p Processor[E]) {
	stats := db.getStats(p)
	stats.TrackStarted()
}

func (db *StatDB[E]) trackFinished(p Processor[E]) {
	stats := db.getStats(p)
	stats.TrackFinished()
}

func (db *StatDB[E]) trackInput(p Processor[E]) {
	stats := db.getStats(p)
	stats.TrackInput()
}

func (db *StatDB[E]) trackOutput(p Processor[E]) {
	stats := db.getStats(p)
	stats.TrackOutput()
}

func (db *StatDB[E]) trackPassthrough(p Processor[E]) {
	stats := db.getStats(p)
	stats.TrackPassthrough()
}

func (s *Stats) TrackStarted() {
	s.Started = time.Now()
}

func (s *Stats) TrackFinished() {
	s.Finished = time.Now()
}

func (s *Stats) TrackOutput() {
	s.LastOutput = time.Now()
	s.Output.Inc()
}

func (s *Stats) TrackPassthrough() {
	s.LastPassthrough = time.Now()
	s.Passthrough.Inc()
}

func (s *Stats) TrackInput() {
	s.LastInput = time.Now()
	s.Input.Inc()
}

func (s *Stats) TrackFailure() {
	s.LastFailure = time.Now()
	s.Failed.Inc()
}
