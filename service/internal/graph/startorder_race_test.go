// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package graph

// Builds a REAL graph with Build() where one receiver is shared by two signal
// pipelines (traces + logs), then runs topo.Sort on the real componentGraph
// exactly as StartAll does, to show the start order is non-deterministic.
//
// Build() gives a shared receiver one node PER SIGNAL (node ID = signal +
// component ID), so the per-signal subgraphs are disconnected:
//
//	recv[traces] -> proc[traces] -> exp[traces]
//	recv[logs]   -> proc[logs]   -> exp[logs]
//
// StartAll does topo.Sort then starts in reverse order. gonum stores nodes in a
// map, so the order BETWEEN disconnected subgraphs is non-deterministic. Because
// the OTLP receiver is a sharedcomponent, the first receiver node to Start brings
// up the single HTTP server for all signals — so if recv[logs] starts before
// proc[traces] (e.g. the traces k8sattributes processor), a trace is served while
// that processor's Start() hasn't run (contrib processor.go:202 nil-deref).
//
// We assert: recv[logs] starts before proc[traces] in some builds and after in
// others (non-deterministic), while a receiver never starts before its OWN
// processor (same-subgraph order is always safe).

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gonum.org/v1/gonum/graph/topo"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/service/internal/builders"
	"go.opentelemetry.io/collector/service/internal/testcomponents"
	"go.opentelemetry.io/collector/service/pipelines"
)

func TestSharedReceiverStartOrderIsNonDeterministic(t *testing.T) {
	recvID := component.MustNewID("examplereceiver")
	procID := component.MustNewID("exampleprocessor")
	expID := component.MustNewID("exampleexporter")

	tracesID := pipeline.NewID(pipeline.SignalTraces)
	logsID := pipeline.NewID(pipeline.SignalLogs)

	// One receiver shared by traces + logs pipelines.
	set := Settings{
		Telemetry: componenttest.NewNopTelemetrySettings(),
		BuildInfo: component.NewDefaultBuildInfo(),
		ReceiverBuilder: builders.NewReceiver(
			map[component.ID]component.Config{recvID: testcomponents.ExampleReceiverFactory.CreateDefaultConfig()},
			map[component.Type]receiver.Factory{testcomponents.ExampleReceiverFactory.Type(): testcomponents.ExampleReceiverFactory}),
		ProcessorBuilder: builders.NewProcessor(
			map[component.ID]component.Config{procID: testcomponents.ExampleProcessorFactory.CreateDefaultConfig()},
			map[component.Type]processor.Factory{testcomponents.ExampleProcessorFactory.Type(): testcomponents.ExampleProcessorFactory}),
		ExporterBuilder: builders.NewExporter(
			map[component.ID]component.Config{expID: testcomponents.ExampleExporterFactory.CreateDefaultConfig()},
			map[component.Type]exporter.Factory{testcomponents.ExampleExporterFactory.Type(): testcomponents.ExampleExporterFactory}),
		ConnectorBuilder: builders.NewConnector(nil, nil),
		PipelineConfigs: pipelines.Config{
			tracesID: {Receivers: []component.ID{recvID}, Processors: []component.ID{procID}, Exporters: []component.ID{expID}},
			logsID:   {Receivers: []component.ID{recvID}, Processors: []component.ID{procID}, Exporters: []component.ID{expID}},
		},
	}

	// Find the real node IDs in the built graph. IDs are content-derived, so
	// they are identical across builds — identify once.
	var recvTraces, recvLogs, procTraces int64
	pg, err := Build(context.Background(), set)
	require.NoError(t, err)
	it := pg.componentGraph.Nodes()
	for it.Next() {
		switch n := it.Node().(type) {
		case *receiverNode:
			switch n.pipelineType {
			case pipeline.SignalTraces:
				recvTraces = n.ID()
			case pipeline.SignalLogs:
				recvLogs = n.ID()
			}
		case *processorNode:
			if n.pipelineID == tracesID {
				procTraces = n.ID()
			}
		}
	}
	require.NotZero(t, recvTraces)
	require.NotZero(t, recvLogs)
	require.NotZero(t, procTraces)

	// startPos rebuilds the graph (fresh map) and replicates StartAll: topo.Sort
	// then reverse, returning each node's start position.
	startPos := func() map[int64]int {
		pg, err := Build(context.Background(), set)
		require.NoError(t, err)
		sorted, err := topo.Sort(pg.componentGraph)
		require.NoError(t, err)
		pos := make(map[int64]int, len(sorted))
		for i, n := range sorted {
			pos[n.ID()] = len(sorted) - 1 - i // reverse = start order
		}
		return pos
	}

	const iters = 5000
	var window, safe int
	for i := 0; i < iters; i++ {
		pos := startPos()
		require.GreaterOrEqual(t, pos[recvTraces], pos[procTraces],
			"invariant broken: receiver started before its own processor")
		if pos[recvLogs] < pos[procTraces] {
			window++ // logs receiver (shared server) up before traces processor
		} else {
			safe++
		}
	}

	t.Logf("iters=%d  panic-window(recvLogs<procTraces)=%d  safe=%d", iters, window, safe)
	require.NotZero(t, window, "ordering looks deterministic (window never hit)")
	require.NotZero(t, safe, "ordering looks deterministic (safe never hit)")
}
