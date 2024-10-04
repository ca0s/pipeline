package pipeline

import (
	"fmt"
	"io"
	"math/rand"
	"strings"
)

type ProcessorGraph[E Traceable] struct {
	root      Processor[E]
	lines     []string
	processed bool
}

func NewProcessorGraph[E Traceable](p Processor[E]) *ProcessorGraph[E] {
	return &ProcessorGraph[E]{
		root:      p,
		lines:     []string{"graph TD"},
		processed: false,
	}
}

func (g *ProcessorGraph[E]) String() string {
	g.process()
	return strings.Join(g.lines, "\n")
}

func (g *ProcessorGraph[E]) process() {
	if g.processed {
		return
	}

	inputID := g.randomID()
	outputID := g.randomID()

	g.lines = append(g.lines, fmt.Sprintf("%s[Input]", inputID))
	g.lines = append(g.lines, fmt.Sprintf("%s[Output]", outputID))

	entryNode, lastNode := g.processInternal(g.root)

	g.lines = append(g.lines, fmt.Sprintf("%s --> %s", inputID, entryNode))
	g.lines = append(g.lines, fmt.Sprintf("%s --> %s", lastNode, outputID))

	g.processed = true
}

func (g *ProcessorGraph[E]) processInternal(node Processor[E]) (string, string) {
	var entryNodeID string
	var outputNodeID string

	switch node.(type) {
	case *Fanout[E]:
		fanout := node.(*Fanout[E])

		entryNodeID = g.randomID()
		outputNodeID = g.randomID()

		g.lines = append(g.lines, fmt.Sprintf("%s[/FanOut/%s\\]", entryNodeID, fanout.ChainName))
		g.lines = append(g.lines, fmt.Sprintf("%s[\\FanOut/%s/end/]", outputNodeID, fanout.ChainName))

		for _, p := range fanout.Processors {
			nodeEntry, nodeOutput := g.processInternal(p)

			g.lines = append(g.lines, fmt.Sprintf("%s --> %s", entryNodeID, nodeEntry))
			g.lines = append(g.lines, fmt.Sprintf("%s --> %s", nodeOutput, outputNodeID))
		}

	case *Parallel[E]:
		parallel := node.(*Parallel[E])

		entryNodeID = g.randomID()
		outputNodeID = g.randomID()

		g.lines = append(g.lines, fmt.Sprintf("%s[/Parallel/%s\\]", entryNodeID, parallel.ChainName))
		g.lines = append(g.lines, fmt.Sprintf("%s[\\Parallel/%s/end/]", outputNodeID, parallel.ChainName))

		for _, p := range parallel.Processors {
			nodeEntry, nodeOutput := g.processInternal(p)

			g.lines = append(g.lines, fmt.Sprintf("%s -.-> %s", entryNodeID, nodeEntry))
			g.lines = append(g.lines, fmt.Sprintf("%s -.-> %s", nodeOutput, outputNodeID))
		}

	case *Sequential[E]:
		seq := node.(*Sequential[E])

		entryNodeID = g.randomID()
		outputNodeID = g.randomID()

		g.lines = append(g.lines, fmt.Sprintf("%s[/Sequential/%s\\]", entryNodeID, seq.ChainName))
		g.lines = append(g.lines, fmt.Sprintf("%s[\\Sequential/%s/end/]", outputNodeID, seq.ChainName))

		prevNode := entryNodeID

		for _, p := range seq.Processors {
			nodeEntry, nodeOutput := g.processInternal(p)

			g.lines = append(g.lines, fmt.Sprintf("%s --> %s", prevNode, nodeEntry))
			prevNode = nodeOutput
		}

		g.lines = append(g.lines, fmt.Sprintf("%s --> %s", prevNode, outputNodeID))

	default:
		nodeID := g.randomID()
		g.lines = append(g.lines, fmt.Sprintf("%s[%s]", nodeID, node.Name()))

		entryNodeID = nodeID
		outputNodeID = nodeID
	}

	return entryNodeID, outputNodeID
}

func (g *ProcessorGraph[E]) randomID() string {
	return fmt.Sprintf("%d", rand.Int())
}

func (g *ProcessorGraph[E]) Write(dest io.Writer) error {
	graph := g.String()

	_, err := dest.Write([]byte(graph))
	return err
}

func (g *ProcessorGraph[E]) WriteHTML(dest io.Writer) error {
	template := fmt.Sprintf(`<html>
    <body>
        <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
        <script>
            mermaid.initialize({ startOnLoad: true, theme: 'dark' });
        </script>

        <div class="mermaid">
            %s
        </div>
    </body>
</html>`, g.String(),
	)

	_, err := dest.Write([]byte(template))
	return err
}
