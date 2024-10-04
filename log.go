package pipeline

import (
	"context"
	"fmt"
	"log"
)

var PipeLineLogLevel PipelineContextKey = "pipeline_log_level"

const PipelineLogLevelDisabled = 0
const PipelineLogLevelDebug = iota

func WithLogLevel(ctx context.Context, level int) context.Context {
	return context.WithValue(ctx, PipeLineLogLevel, level)
}

func Log[E Traceable](ctx context.Context, proc Processor[E], fmts string, args ...interface{}) {
	if ctx.Value(PipeLineLogLevel) == PipelineLogLevelDebug {
		log.Printf("[%s] %s", proc.Name(), fmt.Sprintf(fmts, args...))
	}
}
