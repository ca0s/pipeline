package pipeline

import "context"

type PipelineContextKey string

func Cancelled[E Traceable](ctx context.Context, p Processor[E]) bool {
	if err := ctx.Err(); err != nil {
		Log(ctx, p, "pipeline has been cancelled, stopping %s. reason: %s", p.Name(), err)
		return true
	}

	return false
}
