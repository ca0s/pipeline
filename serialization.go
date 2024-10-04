package pipeline

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type SerializedPipeline[E Traceable] struct {
	Type       string                  `json:"type"`
	Name       string                  `json:"name"`
	Config     map[string]interface{}  `json:"cfg"`
	Processors []SerializedPipeline[E] `json:"processors"`

	processorFactory ProcessorFactory[E]
}

type ProcessorFactory[E Traceable] func(name string, cfg map[string]interface{}) (Processor[E], error)

var ErrInvalidType = fmt.Errorf("invalid pipeline type")

func (sp *SerializedPipeline[E]) Pipeline() (Processor[E], error) {
	switch sp.Type {
	case "fanout":
		fanout := &Fanout[E]{
			ChainName: sp.Name,
		}

		for _, proc := range sp.Processors {
			proc.processorFactory = sp.processorFactory

			builtProc, err := proc.Pipeline()
			if err != nil {
				return nil, err
			}

			fanout.Processors = append(fanout.Processors, builtProc)
		}

		return fanout, nil

	case "parallel":
		parallel := &Parallel[E]{
			ChainName: sp.Name,
		}

		for _, proc := range sp.Processors {
			proc.processorFactory = sp.processorFactory

			builtProc, err := proc.Pipeline()
			if err != nil {
				return nil, err
			}

			parallel.Processors = append(parallel.Processors, builtProc)
		}

		return parallel, nil

	case "sequential":
		sequential := &Sequential[E]{
			ChainName: sp.Name,
		}

		for _, proc := range sp.Processors {
			proc.processorFactory = sp.processorFactory

			builtProc, err := proc.Pipeline()
			if err != nil {
				return nil, err
			}

			sequential.Processors = append(sequential.Processors, builtProc)
		}

		return sequential, nil

	case "processor":
		proc, err := sp.processorFactory(sp.Name, sp.Config)
		if err != nil {
			return nil, fmt.Errorf("%s: %v", sp.Name, ErrInvalidType)
		}

		return proc, nil

	default:
		return nil, ErrInvalidType
	}
}

func (sp *SerializedPipeline[E]) SetProcessorFactory(f ProcessorFactory[E]) {
	sp.processorFactory = f
}

func (item *Sequential[E]) MarshalJSON() ([]byte, error) {
	return marshalPipelineComponent(item.ChainName, "sequential", item.Processors)
}

func (item *Fanout[E]) MarshalJSON() ([]byte, error) {
	return marshalPipelineComponent(item.ChainName, "fanout", item.Processors)
}

func (item *Parallel[E]) MarshalJSON() ([]byte, error) {
	return marshalPipelineComponent(item.ChainName, "parallel", item.Processors)
}

func marshalPipelineComponent[E Traceable](name, typename string, processors []Processor[E]) ([]byte, error) {
	writer := bytes.NewBufferString("")

	writer.WriteString("{")

	writer.WriteString(fmt.Sprintf(`"name": "%s",`, name))
	writer.WriteString(fmt.Sprintf(`"type": "%s",`, typename))

	writer.WriteString(`"processors": [`)

	for pos, processor := range processors {
		var enc []byte
		var err error

		switch processor.(type) {
		case *Parallel[E]:
			enc, err = processor.(*Parallel[E]).MarshalJSON()
		case *Sequential[E]:
			enc, err = processor.(*Sequential[E]).MarshalJSON()
		case *Fanout[E]:
			enc, err = processor.(*Fanout[E]).MarshalJSON()
		default:
			procBuf := bytes.NewBuffer(nil)
			procBuf.WriteString("{")

			procBuf.WriteString(fmt.Sprintf(`"name": "%s", "type": "processor", "cfg": `, processor.Name()))

			cfg, err := json.Marshal(processor)
			if err != nil {
				return nil, err
			}

			if len(cfg) > 0 {
				procBuf.Write(cfg)
			} else {
				procBuf.WriteString("null")
			}

			procBuf.WriteString("}")
			enc = procBuf.Bytes()
		}

		if err != nil {
			return nil, err
		}

		writer.Write(enc)

		if pos < len(processors)-1 {
			writer.WriteString(",")
		}
	}

	writer.WriteString("]")

	writer.WriteString("}")

	return writer.Bytes(), nil
}
