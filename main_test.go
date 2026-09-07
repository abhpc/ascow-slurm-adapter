package main

import "testing"

func TestExtractNodeInfoParsesTypedGpuGres(t *testing.T) {
	info := "NodeName=mxcode2 CPUAlloc=0 CPUTot=20 Gres=gpu:gt720:1 RealMemory=1 AllocMem=0 State=IDLE Partitions=AB,ABGPU AllocTRES="

	node := extractNodeInfo(info)

	if node.GpuCount != 1 {
		t.Fatalf("GpuCount = %d, want 1 for typed Gres gpu:gt720:1", node.GpuCount)
	}
}
