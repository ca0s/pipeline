package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"go.uber.org/zap"
)

func ExtractPipelineAsJSON(pline Processor[Traceable], logger *zap.Logger) {
	d, err := json.Marshal(pline)
	if err != nil {
		logger.Fatal("could not dump pipeline config", zap.Error(err))
	}

	fmt.Printf("%s\n", d)
}

func ExtractPipelineAsGraph(graph string, g *ProcessorGraph[Traceable], logger *zap.Logger) {
	fd, err := os.Create(graph)
	if err != nil {
		logger.Fatal("could not create graph file", zap.String("file", graph), zap.Error(err))
	}

	if strings.HasSuffix(graph, ".html") {
		err = g.WriteHTML(fd)
	} else {
		err = g.Write(fd)
	}

	fd.Close()

	if err != nil {
		logger.Fatal("could not write graph", zap.Error(err))
	}
}
