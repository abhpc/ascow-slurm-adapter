package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	pb "scow-slurm-adapter/gen/go"
	"scow-slurm-adapter/utils"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestGetClusterNodesInfoExpandsAllocation(t *testing.T) {
	// Given: a scheduler expression containing two distinct allocated nodes.
	dir := t.TempDir()
	script := "#!/bin/sh\ncase \"$1 $2\" in\n'show hostnames') printf 'cn01\\ncn02\\n';;\n'show nodes') printf 'NodeName=cn01 NodeAddr=10.0.0.1 NodeHostName=host01 Partitions=p State=IDLE\\nNodeName=cn02 NodeAddr=host02 NodeHostName=host02 Partitions=p State=IDLE\\n';;\n*) exit 1;;\nesac\n"
	if err := os.WriteFile(filepath.Join(dir, "scontrol"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	logger = logrus.New()
	// When
	reply, err := (&serverConfig{}).GetClusterNodesInfo(context.Background(), &pb.GetClusterNodesInfoRequest{NodeNames: []string{"cn[01-02]"}})
	// Then: neither expansion nor scheduler address fields are lost.
	if err != nil {
		t.Fatal(err)
	}
	if len(reply.Nodes) != 2 || reply.Nodes[1].NodeName != "cn02" {
		t.Fatalf("incomplete expansion: %v", reply.Nodes)
	}
	for _, node := range reply.Nodes {
		fields := node.ProtoReflect().Descriptor().Fields()
		for _, number := range []int{14, 15} {
			field := fields.ByNumber(protoreflect.FieldNumber(number))
			if field == nil || node.ProtoReflect().Get(field).String() == "" {
				t.Fatalf("missing scheduler address field %d: %v", number, node)
			}
		}
	}
}

func TestGetClusterNodesInfoRejectsInvalidExpression(t *testing.T) {
	// Given: shell syntax rather than a node expression.
	logger = logrus.New()
	// When
	_, err := (&serverConfig{}).GetClusterNodesInfo(context.Background(), &pb.GetClusterNodesInfoRequest{NodeNames: []string{"cn01;true"}})
	// Then
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got %v, want InvalidArgument", err)
	}
}

func TestSystemUserLookupPreservesNSSInStaticBuild(t *testing.T) {
	// Given: an NSS user not present in /etc/passwd, supplied by the native lookup.
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "getent"), []byte("#!/bin/sh\nprintf 'nss_fixture:x:123456789:1001::/home/nss_fixture:/bin/bash\\n'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	// When
	name, err := utils.GetUserNameByUid(123456789)
	// Then
	if err != nil || name != "nss_fixture" {
		t.Fatalf("NSS lookup = %q, %v", name, err)
	}
}

func TestSystemUserLookupMatchesIntent(t *testing.T) {
	uidGid := func() (string, error) {
		uid, gid, err := utils.GetUserUidGid("1001")
		return fmt.Sprintf("%d:%d", uid, gid), err
	}
	home := func() (string, error) { return utils.GetUserHomedir("1001") }
	name := func() (string, error) { return utils.GetUserNameByUid(1001) }
	const uidRecord = "other:x:1001:2001::/home/other:/bin/bash"
	const nameRecord = "1001:x:3001:2001::/home/1001:/bin/bash"
	tests := []struct {
		name   string
		record string
		lookup func() (string, error)
		want   string
	}{
		{"numeric username UID collision", uidRecord, uidGid, ""},
		{"numeric home username UID collision", uidRecord, home, ""},
		{"UID mismatch despite matching username", nameRecord, name, ""},
		{"exact numeric username UID GID", nameRecord, uidGid, "3001:2001"},
		{"exact numeric username home", nameRecord, home, "/home/1001"},
		{"exact UID with distinct username", uidRecord, name, "other"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Given: keyed getent returns either a numeric name or the colliding UID account.
			dir := t.TempDir()
			script := "#!/bin/sh\n[ \"$#\" -eq 3 ] && [ \"$1\" = passwd ] && [ \"$2\" = -- ] && [ \"$3\" = 1001 ] || exit 1\nprintf '%s\\n' '" + test.record + "'\n"
			if err := os.WriteFile(filepath.Join(dir, "getent"), []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
			// When
			got, err := test.lookup()
			// Then: accept only the record matching the API's lookup intent.
			if test.want == "" {
				if err == nil {
					t.Fatalf("mismatched record accepted: %q", got)
				}
			} else if err != nil || got != test.want {
				t.Fatalf("lookup = %q, %v; want %q", got, err, test.want)
			}
		})
	}
}

func TestGetClusterNodesInfoAddressPolicy(t *testing.T) {
	tests := []struct {
		name   string
		record string
		want   string
		code   codes.Code
	}{
		{"IPv6", "NodeName=cn01 NodeAddr=2001:db8::1 NodeHostName=host01", "2001:db8::1", codes.OK},
		{"IPv6 loopback", "NodeName=cn01 NodeAddr=::1 NodeHostName=host01", "::1", codes.OK},
		{"IPv4 mapped IPv6", "NodeName=cn01 NodeAddr=::ffff:10.0.0.1 NodeHostName=host01", "::ffff:10.0.0.1", codes.OK},
		{"IPv4", "NodeName=cn01 NodeAddr=10.0.0.1 NodeHostName=host01", "10.0.0.1", codes.OK},
		{"hostname", "NodeName=cn01 NodeAddr=host-01.example NodeHostName=host01", "host-01.example", codes.OK},
		{"missing addresses", "NodeName=cn01", "", codes.OK},
		{"malformed IPv6", "NodeName=cn01 NodeAddr=2001:db8:::1 NodeHostName=host01", "", codes.Internal},
		{"address with port", "NodeName=cn01 NodeAddr=[2001:db8::1]:22 NodeHostName=host01", "", codes.Internal},
		{"hostname injection", "NodeName=cn01 NodeAddr=host01;true NodeHostName=host01", "", codes.Internal},
		{"strict node hostname", "NodeName=cn01 NodeAddr=::1 NodeHostName=2001:db8::1", "", codes.Internal},
		{"strict node name", "NodeName=2001:db8::1 NodeAddr=::1 NodeHostName=host01", "", codes.Internal},
		{"missing node record", "", "", codes.Internal},
		{"duplicate address", "NodeName=cn01 NodeAddr=::1 NodeAddr=::2 NodeHostName=host01", "", codes.Internal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Given: the scheduler returns one allocated node with the supplied metadata.
			dir := t.TempDir()
			script := "#!/bin/sh\ncase \"$1 $2\" in\n'show hostnames') printf 'cn01\\n';;\n'show nodes') printf '%s\\n' '" + test.record + "';;\n*) exit 1;;\nesac\n"
			if err := os.WriteFile(filepath.Join(dir, "scontrol"), []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
			// When
			reply, err := (&serverConfig{}).GetClusterNodesInfo(context.Background(), &pb.GetClusterNodesInfoRequest{NodeNames: []string{"cn01"}})
			// Then: preserve valid address metadata and the existing record failure policy.
			if status.Code(err) != test.code {
				t.Fatalf("got %v, want %v", err, test.code)
			}
			if test.code == codes.OK && (len(reply.Nodes) != 1 || reply.Nodes[0].NodeAddr != test.want) {
				t.Fatalf("address metadata lost: %v", reply.Nodes)
			}
		})
	}
}

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

func TestParsePartitionGpuStatsMarksSharedGpuUsedInEveryPartition(t *testing.T) {
	info := "NodeName=node01 CPUAlloc=10 CPUTot=16 Gres=gpu:1 GresUsed=gpu:1 " +
		"RealMemory=30000 AllocMem=18750 State=MIXED Partitions=local,Partition2"

	got := parsePartitionGpuStats(info)
	want := partitionGpuStats{total: 1, running: 1}

	for _, partition := range []string{"local", "Partition2"} {
		if got[partition] != want {
			t.Fatalf("parsePartitionGpuStats()[%q] = %+v, want %+v", partition, got[partition], want)
		}
	}
}
