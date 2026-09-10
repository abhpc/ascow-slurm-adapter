package main

import (
	"strings"
	"testing"
)

func TestParseGpuCount(t *testing.T) {
	tests := []struct {
		gres string
		want int
	}{
		{"", 0},
		{"(null)", 0},
		{"Gres=(null)", 0},
		{"gt720", 1},
		{"gpu:gt720", 1},
		{"gpu:1", 1},
		{"gpu:gt720:2", 2},
		{"gpu:gt720:2(S:0-1)", 2},
	}

	for _, test := range tests {
		t.Run(test.gres, func(t *testing.T) {
			if got := parseGpuCount(test.gres); got != test.want {
				t.Fatalf("parseGpuCount(%q) = %d, want %d", test.gres, got, test.want)
			}
		})
	}
}

func TestExtractNodeInfoParsesTypedGpuGres(t *testing.T) {
	info := "NodeName=mxcode2 CPUAlloc=0 CPUTot=20 Gres=gpu:gt720:1 RealMemory=1 AllocMem=0 State=IDLE Partitions=AB,ABGPU AllocTRES="

	node := extractNodeInfo(info)

	if node.GpuCount != 1 {
		t.Fatalf("GpuCount = %d, want 1 for typed Gres gpu:gt720:1", node.GpuCount)
	}
}

func TestParseClusterInfoTotalsDeduplicatesNodesAcrossPartitions(t *testing.T) {
	sinfo := strings.Join([]string{
		"node1|AB*|1/0/0/1|4/16/0/20|gpu:gt720",
		"node1|ABGPU|1/0/0/1|4/16/0/20|gpu:gt720",
		"node2|AB*|0/0/1/1|0/0/20/20|gpu:gt720",
		"node2|ABGPU|0/0/1/1|0/0/20/20|gpu:gt720",
	}, "\n")
	squeue := strings.Join([]string{
		"1|RUNNING|gpu:1|1",
		"2_1|PENDING|N/A|1",
		"2_2|PENDING|N/A|1",
	}, "\n")

	got, err := parseClusterInfoTotals(sinfo, squeue)
	if err != nil {
		t.Fatal(err)
	}
	want := clusterInfoTotals{
		nodes: [4]uint32{1, 0, 1, 2},
		cpus:  [4]uint32{4, 16, 20, 40},
		gpus:  [4]uint32{1, 0, 1, 2},
		jobs:  [2]uint32{1, 2},
	}
	if got != want {
		t.Fatalf("parseClusterInfoTotals() = %+v, want %+v", got, want)
	}
}

func TestParseClusterInfoTotalsRejectsInconsistentDuplicateNode(t *testing.T) {
	sinfo := strings.Join([]string{
		"node1|AB*|0/1/0/1|0/20/0/20|gpu:gt720",
		"node1|ABGPU|0/0/1/1|0/0/20/20|gpu:gt720",
	}, "\n")

	if _, err := parseClusterInfoTotals(sinfo, ""); err == nil {
		t.Fatal("parseClusterInfoTotals() succeeded for inconsistent duplicate node")
	}
}
