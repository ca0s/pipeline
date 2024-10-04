package pipeline

import (
	"context"
	"fmt"
	"sync"
)

/*
	A processor is the basic block of this library. An implementation should:

	- Read all items from its input channel
	- Do whatever processing it needs to do
	- Send output to its output channel
	- Close its output channel when finished. If this final step is not performed,
		the whole pipeline will hang.

	The caller of Execute MUST close the input channel when all data is written
	The caller of Execute MUST NOT close the output channel. It will be closed by the Processor
*/
type Processor[E Traceable] interface {
	Execute(ctx context.Context, input chan E, output chan E)
	Name() string
}

/*
	The Fanout processor has:

	- One input
	- X processors
	- One output

	Input is forwarded to ALL processors. Their output is collected and forwarded
	to the Fanout output.
*/
type Fanout[E Traceable] struct {
	ChainName string

	Processors   []Processor[E]
	procInChans  []chan E
	procOutChans []chan E
}

/*
	The Sequential processor has:

	- One input
	- X processors
	- One output

	Input is forwarded to the first processor, which in turns sends its output
	to the next one in the list sequentially.

	The output of the last processor is collected and sent to the Sequential output.
*/
type Sequential[E Traceable] struct {
	ChainName string

	Processors   []Processor[E]
	procOutChans []chan E
}

/*
	The Parallel processor has:

	- One input
	- X processors
	- One output

	Each item coming from the input is forwarded to the first available processor.

	Processor's output is collected and forwarded to the Parallel output.
*/
type Parallel[E Traceable] struct {
	ChainName string

	Processors []Processor[E]
	procChans  []chan E
}

func (fanout *Fanout[E]) Execute(ctx context.Context, input chan E, output chan E) {
	Log[E](ctx, fanout, "starting")
	TrackStarted[E](ctx, fanout)

	if len(fanout.Processors) == 0 {
		close(output)
		return
	}

	wg := sync.WaitGroup{}
	collectorWg := sync.WaitGroup{}

	fanout.procInChans = make([]chan E, len(fanout.Processors))
	fanout.procOutChans = make([]chan E, len(fanout.Processors))

	fanoutCollector := make(chan E)

	collectorWg.Add(1)
	go func() {
		for m := range fanoutCollector {
			TrackOutput[E](ctx, fanout, m)
			output <- m
		}
		collectorWg.Done()
	}()

	for procIndex, proc := range fanout.Processors {
		procInput := make(chan E, 200)
		procOutput := make(chan E, 200)

		fanout.procInChans[procIndex] = procInput
		fanout.procOutChans[procIndex] = procOutput

		wg.Add(1)
		go func(p Processor[E]) {
			p.Execute(ctx, procInput, procOutput)
			wg.Done()
		}(proc)

		wg.Add(1)
		go func() {
			for m := range procOutput {
				fanoutCollector <- m
			}
			wg.Done()
		}()
	}

	wg.Add(1)
	go func() {
		for msg := range input {
			TrackInput[E](ctx, fanout)

			for _, procInput := range fanout.procInChans {
				procInput <- msg
			}
		}

		for _, procInput := range fanout.procInChans {
			close(procInput)
		}

		wg.Done()
	}()

	wg.Wait()

	close(fanoutCollector)
	collectorWg.Wait()

	TrackFinished[E](ctx, fanout)
	close(output)
}

func (fanout *Fanout[E]) Name() string {
	return fmt.Sprintf("Fanout/%s", fanout.ChainName)
}

func (chain *Sequential[E]) Execute(ctx context.Context, input chan E, output chan E) {
	Log[E](ctx, chain, "starting")
	TrackStarted[E](ctx, chain)

	if len(chain.Processors) == 0 {
		close(output)
		return
	}

	wg := sync.WaitGroup{}

	lastIndex := len(chain.Processors) - 1
	chain.procOutChans = make([]chan E, len(chain.Processors))

	var entryChannel chan E

	for procIndex, proc := range chain.Processors {
		var procInput chan E
		var procOutput chan E

		if procIndex == 0 {
			procInput = make(chan E)
			entryChannel = procInput
		} else {
			procInput = chain.procOutChans[procIndex-1]
		}

		procOutput = make(chan E)

		chain.procOutChans[procIndex] = procOutput

		wg.Add(1)
		go func(s Processor[E]) {
			s.Execute(ctx, procInput, procOutput)
			wg.Done()
		}(proc)
	}

	wg.Add(1)
	go func() {
		for msg := range input {
			TrackInput[E](ctx, chain)
			entryChannel <- msg
		}

		close(entryChannel)

		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for m := range chain.procOutChans[lastIndex] {
			TrackOutput[E](ctx, chain, m)
			output <- m
		}
		wg.Done()
	}()

	wg.Wait()

	TrackFinished[E](ctx, chain)
	close(output)
}

func (sequential *Sequential[E]) Name() string {
	return fmt.Sprintf("Sequential/%s", sequential.ChainName)
}

func (chain *Parallel[E]) Execute(ctx context.Context, input chan E, output chan E) {
	Log[E](ctx, chain, "starting")
	TrackStarted[E](ctx, chain)

	if len(chain.Processors) == 0 {
		close(output)
		return
	}

	wg := sync.WaitGroup{}

	for procIndex, proc := range chain.Processors {
		procOutput := make(chan E)
		chain.procChans[procIndex] = procOutput

		wg.Add(1)
		go func() {
			proc.Execute(ctx, input, procOutput)
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			for m := range procOutput {
				TrackOutput[E](ctx, chain, m)
				output <- m
			}
			wg.Done()
		}()
	}

	wg.Wait()

	TrackFinished[E](ctx, chain)
	close(output)
}

func (parallel *Parallel[E]) Name() string {
	return fmt.Sprintf("Parallel/%s", parallel.ChainName)
}
