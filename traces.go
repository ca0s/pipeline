package pipeline

import (
	"context"
)

var TracesFlag PipelineContextKey = "traces_flag"

func WithTraces(ctx context.Context) context.Context {
	return context.WithValue(ctx, TracesFlag, true)
}

func HasTracesEnabled(ctx context.Context) bool {
	return ctx.Value(TracesFlag) == true
}

type Traceable interface {
	AddTrace(string)
}
