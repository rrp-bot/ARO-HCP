// Copyright 2025 Microsoft Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package entrypointutils

import (
	"fmt"
	"strconv"

	"github.com/Azure/ARO-Tools/config"
	configtypes "github.com/Azure/ARO-Tools/config/types"
	"github.com/Azure/ARO-Tools/pipelines/graph"
	"github.com/Azure/ARO-Tools/pipelines/topology"
	"github.com/Azure/ARO-Tools/pipelines/types"
)

// BuildStampList determines which stamps to process.
// --stamp-count-config-ref wins over --stamp (which may come from dev settings).
func BuildStampList(stamp, stampCountConfigRef string, cfg configtypes.Configuration) ([]string, error) {
	if len(stampCountConfigRef) > 0 {
		rawCount, err := cfg.GetByPath(stampCountConfigRef)
		if err != nil {
			return nil, fmt.Errorf("failed to read stamp count from config path %q: %w", stampCountConfigRef, err)
		}

		var stampCount int
		switch v := rawCount.(type) {
		case int:
			stampCount = v
		case float64:
			if v != float64(int(v)) {
				return nil, fmt.Errorf("stamp count at %q must be an integer, got %v", stampCountConfigRef, v)
			}
			stampCount = int(v)
		default:
			return nil, fmt.Errorf("stamp count at %q is %T, expected int", stampCountConfigRef, rawCount)
		}

		if stampCount < 1 {
			return nil, fmt.Errorf("stamp count at %q must be >= 1, got %d", stampCountConfigRef, stampCount)
		}

		stamps := make([]string, stampCount)
		for i := range stampCount {
			stamps[i] = strconv.Itoa(i + 1)
		}
		return stamps, nil
	}

	return []string{stamp}, nil
}

// ResolveStampConfigs resolves per-stamp configurations from the config resolver.
func ResolveStampConfigs(stamps []string, resolver config.ConfigResolver, region string, regionShortReplacement string) (map[string]configtypes.Configuration, error) {
	configs := make(map[string]configtypes.Configuration, len(stamps))
	for _, stamp := range stamps {
		stampCfg, err := resolver.GetRegionConfiguration(region, stamp, regionShortReplacement)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve config for stamp %s: %w", stamp, err)
		}
		configs[stamp] = stampCfg
	}
	return configs, nil
}

// BuildStampedGraph builds a stamp-aware execution graph. For entrypoint runs it
// uses graph.ForStampedEntrypoints which expands stamped services once per stamp
// during graph construction. Returns the graph along with per-stamp configs and
// pipelines needed for execution.
func (o *Options) BuildStampedGraph() (*graph.Graph, map[string]configtypes.Configuration, map[string]map[string]*types.Pipeline, error) {
	stamps, err := BuildStampList(o.Stamp, o.StampCountConfigRef, o.Config)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to build stamp list: %w", err)
	}

	stampConfigs, err := ResolveStampConfigs(stamps, o.ConfigResolver, o.Region, o.RegionShort)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to resolve stamp configs: %w", err)
	}

	stampPipelines := map[string]map[string]*types.Pipeline{}
	for stamp, cfg := range stampConfigs {
		pipelines := map[string]*types.Pipeline{}
		if err := LoadPipelines(o.Service, o.Topo, pipelines, cfg); err != nil {
			return nil, nil, nil, fmt.Errorf("failed to load pipelines for stamp %s: %w", stamp, err)
		}
		stampPipelines[stamp] = pipelines
	}

	var executionGraph *graph.Graph
	if o.Entrypoint != nil {
		executionGraph, err = graph.ForStampedEntrypoints(&o.Topo.Topology, []*topology.Entrypoint{o.Entrypoint}, stampPipelines)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to generate stamped execution graph: %w", err)
		}
	} else {
		firstStamp := stamps[0]
		executionGraph, err = graph.ForPipeline(o.Service, stampPipelines[firstStamp][o.Service.ServiceGroup])
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to generate execution graph: %w", err)
		}
	}

	return executionGraph, stampConfigs, stampPipelines, nil
}
