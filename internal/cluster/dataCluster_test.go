/*
 * Copyright (c) Marco Tusa 2021 - present
 *                     GNU GENERAL PUBLIC LICENSE
 *                        Version 3, 29 June 2007
 *
 *  Copyright (C) 2007 Free Software Foundation, Inc. <https://fsf.org/>
 *  Everyone is permitted to copy and distribute verbatim copies
 *  of this license document, but changing it is not allowed.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package cluster

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"testing"

	global "pxc_scheduler_handler/internal/global"

	log "github.com/sirupsen/logrus"
	"hegel.dev/go/hegel"
)

func TestMain(m *testing.M) {
	log.SetLevel(log.PanicLevel)
	os.Exit(m.Run())
}

// ****************  helpers **********************************

// newTestCluster returns a cluster with writer HG 100, reader HG 101,
// config HGs 8100/8101 and maintenance HGs 9100/9101.
func newTestCluster() *DataClusterImpl {
	tc := testClusterFactory()
	cluster := tc.clusterNodeImplFactory()
	cluster.NodesPxc = NewRegularIntMap()
	cluster.Hostgroups = map[int]Hostgroup{
		100:  getHgOpt(100, 1, "W"),
		101:  getHgOpt(101, 3, "R"),
		9100: getHgOpt(9100, 0, "WREC"),
		9101: getHgOpt(9101, 0, "RREC"),
	}
	return cluster
}

// healthyNode builds a node whose derived wsrep fields come from realistic
// variables/status maps, the same way getInfo populates them.
func healthyNode(dns string, hg int, weight int) DataNodeImpl {
	node := testClusterFactory().testDataNodeFactoryDns(dns)
	node.HostgroupId = hg
	node.Weight = weight
	node.Variables = map[string]string{
		"pxc_maint_mode":                  "DISABLED",
		"wsrep_sst_donor_rejects_queries": "OFF",
		"wsrep_provider_options":          "gmcast.segment = 1; pc.weight = 1",
		"wsrep_cluster_name":              "test",
		"wsrep_node_name":                 dns,
		"wsrep_reject_queries":            "NONE",
		"read_only":                       "OFF",
	}
	node.Status = map[string]string{
		"wsrep_connected":        "ON",
		"wsrep_desync_count":     "0",
		"wsrep_gcomm_uuid":       "uuid-" + dns,
		"wsrep_cluster_status":   "Primary",
		"wsrep_local_recv_queue": "0",
		"wsrep_cluster_size":     "3",
		"wsrep_ready":            "ON",
		"wsrep_local_state":      "4",
	}
	node.setParameters()
	return node
}

// addNode registers a node in NodesPxc and in the given role maps with the proper HG.
func addNode(cluster *DataClusterImpl, dns string, weight int, writer bool) DataNodeImpl {
	node := healthyNode(dns, cluster.HgReaderId, weight)
	cluster.NodesPxc.Store(dns, node)
	cluster.ReaderNodes[dns] = node

	bkR := node
	bkR.HostgroupId = cluster.BackupHgReaderId
	cluster.BackupReaders[dns] = bkR

	bkW := node
	bkW.HostgroupId = cluster.BakcupHgWriterId
	cluster.BackupWriters[dns] = bkW

	if writer {
		w := node
		w.HostgroupId = cluster.HgWriterId
		cluster.WriterNodes[dns] = w
	}
	return node
}

// ****************  pure helpers **********************************

func TestSyncMap(t *testing.T) {
	m := NewRegularIntMap()
	if _, ok := m.Load("a"); ok {
		t.Fatal("Load() on empty map returned ok")
	}
	m.Store("a", DataNodeImpl{Dns: "a"})
	if v, ok := m.Load("a"); !ok || v.Dns != "a" {
		t.Errorf("Load() = %v, %v", v, ok)
	}
	if len(m.ExposeMap()) != 1 {
		t.Errorf("ExposeMap() len = %d", len(m.ExposeMap()))
	}
	m.Delete("a")
	if _, ok := m.Load("a"); ok {
		t.Error("Load() after Delete() returned ok")
	}
}

func TestMergeMapsAndCopyMap(t *testing.T) {
	w := map[string]DataNodeImpl{"n1": {Dns: "n1", HostgroupId: 100}}
	r := map[string]DataNodeImpl{"n1": {Dns: "n1", HostgroupId: 101}, "n2": {Dns: "n2", HostgroupId: 101}}
	merged := MergeMaps([4]map[string]DataNodeImpl{w, r, nil, {}})
	want := []string{"100_n1", "101_n1", "101_n2"}
	if len(merged) != len(want) {
		t.Fatalf("MergeMaps() = %v", merged)
	}
	for _, k := range want {
		if _, ok := merged[k]; !ok {
			t.Errorf("MergeMaps() missing key %s", k)
		}
	}

	dst := map[string]DataNodeImpl{"x": {Dns: "x"}}
	got := CopyMap(dst, r)
	if len(got) != 3 || got["n2"].Dns != "n2" {
		t.Errorf("CopyMap() = %v", got)
	}
}

func TestCalculateHgOnlineSizeAndConsolidateHGs(t *testing.T) {
	cluster := newTestCluster()
	cluster.WriterNodes = map[string]DataNodeImpl{"a": {ProxyStatus: "ONLINE"}}
	cluster.ReaderNodes = map[string]DataNodeImpl{
		"a": {ProxyStatus: "ONLINE"},
		"b": {ProxyStatus: "ONLINE"},
		"c": {ProxyStatus: "OFFLINE_SOFT"},
	}
	cluster.OffLineReaders = map[string]DataNodeImpl{"d": {ProxyStatus: "ONLINE"}}

	if got := cluster.calculateHgOnlineSize(cluster.ReaderNodes); got != 2 {
		t.Errorf("calculateHgOnlineSize() = %d, want 2", got)
	}

	cluster.consolidateHGs()
	want := map[int]struct {
		typ  string
		size int
	}{100: {"W", 1}, 101: {"R", 2}, 9100: {"WREC", 0}, 9101: {"RREC", 1}}
	if len(cluster.Hostgroups) != len(want) {
		t.Fatalf("consolidateHGs() = %v", cluster.Hostgroups)
	}
	for id, w := range want {
		hg := cluster.Hostgroups[id]
		if hg.Id != id || hg.Type != w.typ || hg.Size != w.size {
			t.Errorf("Hostgroups[%d] = %+v, want type %s size %d", id, hg, w.typ, w.size)
		}
	}
}

func TestSetParameters(t *testing.T) {
	node := healthyNode("n1:3306", 100, 1)
	if node.PxcMaintMode != "DISABLED" || !node.WsrepConnected || node.WsrepDesinccount != 0 ||
		node.WsrepDonorrejectqueries || node.WsrepGcommUuid != "uuid-n1:3306" || !node.HasPrimaryState ||
		node.WsrepClusterName != "test" || node.WsrepClusterStatus != "Primary" || node.WsrepNodeName != "n1:3306" ||
		node.WsrepClusterSize != 3 || node.WsrepPcWeight != 1 || !node.WsrepReady || node.WsrepRejectqueries ||
		node.WsrepSegment != 1 || node.WsrepStatus != 4 || node.ReadOnly {
		t.Errorf("setParameters() produced unexpected node: %+v", node)
	}

	node.Variables["read_only"] = "ON"
	node.Variables["wsrep_reject_queries"] = "ALL"
	node.Status["wsrep_local_state"] = "2"
	node.setParameters()
	if !node.ReadOnly || !node.WsrepRejectqueries || node.WsrepStatus != 2 {
		t.Errorf("setParameters() after change: ReadOnly=%v Reject=%v Status=%d", node.ReadOnly, node.WsrepRejectqueries, node.WsrepStatus)
	}
}

func TestAlignNodeValues(t *testing.T) {
	cluster := newTestCluster()
	src := healthyNode("n1:3306", 101, 1)
	src.Variables["pxc_maint_mode"] = "MAINTENANCE"
	src.Processed = true
	src.ActionType = 3010

	dst := DataNodeImpl{Dns: "n1:3306", HostgroupId: 8101, Weight: 55}
	got := cluster.alignNodeValues(dst, src)
	if got.HostgroupId != 8101 || got.Weight != 55 {
		t.Errorf("alignNodeValues() overwrote identity fields: %+v", got)
	}
	if got.PxcMaintMode != "MAINTENANCE" || !got.Processed || got.ActionType != 3010 || got.WsrepStatus != 4 {
		t.Errorf("alignNodeValues() did not copy status: %+v", got)
	}
}

func TestConsolidateNodes(t *testing.T) {
	cluster := newTestCluster()
	known := healthyNode("a:3306", 100, 1)
	cluster.WriterNodes["a:3306"] = DataNodeImpl{Dns: "a:3306", HostgroupId: 100}
	cluster.BackupWriters["a:3306"] = DataNodeImpl{Dns: "a:3306", HostgroupId: 8100}
	cluster.BackupReaders["a:3306"] = DataNodeImpl{Dns: "a:3306", HostgroupId: 8101}
	cluster.OffLineWriters["a:3306"] = DataNodeImpl{Dns: "a:3306", HostgroupId: 9100}
	cluster.OffLineReaders["a:3306"] = DataNodeImpl{Dns: "a:3306", HostgroupId: 9101}
	cluster.NodesPxc.Store("a:3306", known)

	unknown := healthyNode("b:3306", 0, 1)
	cluster.NodesPxc.Store("b:3306", unknown)
	cluster.HasPrimary = false

	if !cluster.consolidateNodes() {
		t.Fatal("consolidateNodes() = false")
	}
	for name, m := range map[string]map[string]DataNodeImpl{
		"writer": cluster.WriterNodes, "bkW": cluster.BackupWriters, "bkR": cluster.BackupReaders,
		"offW": cluster.OffLineWriters, "offR": cluster.OffLineReaders,
	} {
		if m["a:3306"].WsrepStatus != 4 {
			t.Errorf("%s node not aligned: %+v", name, m["a:3306"])
		}
	}
	if _, ok := cluster.ReaderNodes["a:3306"]; ok {
		t.Error("known node should not be added to readers")
	}
	if _, ok := cluster.ReaderNodes["b:3306"]; !ok {
		t.Error("unknown node should be added to readers")
	}
	if !cluster.HasPrimary || cluster.Size != 3 || cluster.Name != "test" {
		t.Errorf("cluster params not aligned: HasPrimary=%v Size=%d Name=%s", cluster.HasPrimary, cluster.Size, cluster.Name)
	}
}

func TestGetRetry(t *testing.T) {
	node := DataNodeImpl{Comment: "keep 100_W_101_R_retry_up=3;100_W_101_R_retry_down=2;me"}
	node.getRetry(100, 101)
	if node.RetryUp != 3 || node.RetryDown != 2 {
		t.Errorf("getRetry() up=%d down=%d, want 3/2", node.RetryUp, node.RetryDown)
	}
	if node.Comment != "keep me" {
		t.Errorf("getRetry() comment = %q, want %q", node.Comment, "keep me")
	}

	other := DataNodeImpl{Comment: "200_W_201_R_retry_up=3;"}
	other.getRetry(100, 101)
	if other.RetryUp != 0 || other.Comment != "200_W_201_R_retry_up=3;" {
		t.Errorf("getRetry() parsed retry for a different HG pair: %+v", other)
	}
}

func TestActionCodes(t *testing.T) {
	n := &DataNodeImpl{}
	tests := []struct {
		code     int
		text     string
		category string
	}{
		{n.NOTHING_TO_DO(), "NOTHING_TO_DO", "NOTHING_TO_DO"},
		{n.MOVE_UP_OFFLINE(), "MOVE_UP_OFFLINE", "UP"},
		{n.MOVE_UP_HG_CHANGE(), "MOVE_UP_HG_CHANGE", "UP"},
		{n.RESET_DEFAULTS(), "RESET_DEFAULTS", ""},
		{n.MOVE_DOWN_HG_CHANGE(), "MOVE_DOWN_HG_CHANGE", "DOWN"},
		{n.MOVE_DOWN_OFFLINE(), "MOVE_DOWN_OFFLINE", "DOWN"},
		{n.MOVE_TO_MAINTENANCE(), "MOVE_TO_MAINTENANCE", "DOWN"},
		{n.MOVE_OUT_MAINTENANCE(), "MOVE_OUT_MAINTENANCE", "UP"},
		{n.INSERT_READ(), "INSERT_READ", "UP"},
		{n.INSERT_WRITE(), "INSERT_WRITE", "UP"},
		{n.DELETE_NODE(), "DELETE_NODE", "DOWN"},
		{n.MOVE_SWAP_READER_TO_WRITER(), "MOVE_SWAP_READER_TO_WRITER", "SWAP_W"},
		{n.MOVE_SWAP_WRITER_TO_READER(), "MOVE_SWAP_WRITER_TO_READER", "SWAP_R"},
		{n.SAVE_RETRY(), "SAVE_RETRY", "SAVE_RETRY"},
		{12345, "", ""},
	}
	for _, tt := range tests {
		if got := n.ReturnTextFromCode(tt.code); got != tt.text {
			t.Errorf("ReturnTextFromCode(%d) = %q, want %q", tt.code, got, tt.text)
		}
		if got := n.ReturnActionCategory(tt.code); got != tt.category {
			t.Errorf("ReturnActionCategory(%d) = %q, want %q", tt.code, got, tt.category)
		}
	}
}

func TestResetAndCompareNodeDefault(t *testing.T) {
	cluster := newTestCluster()
	def := DataNodeImpl{Weight: 10, Compression: 1, MaxConnection: 200, MaxReplicationLag: 5, MaxLatency: 7}
	in := DataNodeImpl{Dns: "a", Weight: 1}

	if !cluster.compareNodeDefault(in, def) {
		t.Error("compareNodeDefault() = false for differing nodes")
	}
	out := cluster.resetNodeDefault(in, def)
	if out.Dns != "a" || out.Weight != 10 || out.Compression != 1 || out.MaxConnection != 200 ||
		out.MaxReplicationLag != 5 || out.MaxLatency != 7 {
		t.Errorf("resetNodeDefault() = %+v", out)
	}
	if cluster.compareNodeDefault(out, def) {
		t.Error("compareNodeDefault() = true for equal settings")
	}
}

// ****************  per-node evaluation **********************************

func TestEvaluateNode(t *testing.T) {
	t.Run("unprocessed node untouched", func(t *testing.T) {
		cluster := newTestCluster()
		node := healthyNode("a:3306", 100, 1)
		node.Processed = false
		node.Variables["pxc_maint_mode"] = "MAINTENANCE"
		node.setParameters()
		cluster.evaluateNode(node)
		if len(cluster.ActionNodes) != 0 {
			t.Errorf("ActionNodes = %v", cluster.ActionNodes)
		}
	})
	t.Run("node in unmanaged HG untouched", func(t *testing.T) {
		cluster := newTestCluster()
		node := healthyNode("a:3306", 500, 1)
		node.WsrepClusterStatus = "non-Primary"
		cluster.evaluateNode(node)
		if len(cluster.ActionNodes) != 0 {
			t.Errorf("ActionNodes = %v", cluster.ActionNodes)
		}
	})
	t.Run("pxc_maint_mode moves node offline", func(t *testing.T) {
		cluster := newTestCluster()
		node := healthyNode("a:3306", 100, 1)
		node.Variables["pxc_maint_mode"] = "MAINTENANCE"
		node.setParameters()
		cluster.evaluateNode(node)
		got, ok := cluster.ActionNodes["100_a:3306"]
		if !ok || got.ActionType != node.MOVE_DOWN_OFFLINE() || got.RetryDown != 1 {
			t.Errorf("ActionNodes = %v", cluster.ActionNodes)
		}
	})
	t.Run("not primary moves node to maintenance HG", func(t *testing.T) {
		cluster := newTestCluster()
		node := healthyNode("a:3306", 101, 1)
		node.WsrepClusterStatus = "non-Primary"
		cluster.evaluateNode(node)
		if got := cluster.ActionNodes["101_a:3306"]; got.ActionType != node.MOVE_DOWN_HG_CHANGE() {
			t.Errorf("ActionNodes = %v", cluster.ActionNodes)
		}
	})
	t.Run("offline soft node comes back", func(t *testing.T) {
		cluster := newTestCluster()
		node := healthyNode("a:3306", 101, 1)
		node.ProxyStatus = "OFFLINE_SOFT"
		cluster.evaluateNode(node)
		if got := cluster.ActionNodes["101_a:3306"]; got.ActionType != node.MOVE_UP_OFFLINE() {
			t.Errorf("ActionNodes = %v", cluster.ActionNodes)
		}
	})
	t.Run("read only writer in offline soft stays put", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.RetryDown = 0
		node := healthyNode("a:3306", 100, 1)
		node.ProxyStatus = "OFFLINE_SOFT"
		node.ReadOnly = true
		// checkReadOnly fires first for writers
		cluster.evaluateNode(node)
		if got := cluster.ActionNodes["100_a:3306"]; got.ActionType != node.MOVE_SWAP_WRITER_TO_READER() {
			t.Errorf("ActionNodes = %v", cluster.ActionNodes)
		}
	})
	t.Run("healthy node in maintenance HG comes back", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.RetryUp = 1
		node := healthyNode("a:3306", 9101, 1)
		cluster.evaluateNode(node)
		got := cluster.ActionNodes["9101_a:3306"]
		if got.ActionType != node.MOVE_UP_HG_CHANGE() || got.RetryUp != 1 {
			t.Errorf("ActionNodes = %v", cluster.ActionNodes)
		}
	})
	t.Run("healthy node with retries resets counters", func(t *testing.T) {
		cluster := newTestCluster()
		node := healthyNode("a:3306", 101, 1)
		node.RetryUp = 2
		node.RetryDown = 1
		cluster.evaluateNode(node)
		got := cluster.ActionNodes["101_a:3306"]
		if got.ActionType != node.SAVE_RETRY() || got.RetryUp != 0 || got.RetryDown != 0 {
			t.Errorf("ActionNodes = %v", cluster.ActionNodes)
		}
	})
	t.Run("healthy node without retries does nothing", func(t *testing.T) {
		cluster := newTestCluster()
		node := healthyNode("a:3306", 101, 1)
		if cluster.checkUpSaveRetry(node, cluster.Hostgroups[101]) {
			t.Error("checkUpSaveRetry() = true")
		}
		cluster.evaluateNode(node)
		if len(cluster.ActionNodes) != 0 {
			t.Errorf("ActionNodes = %v", cluster.ActionNodes)
		}
	})
}

func TestEvaluateAllProcessedNodes_Shunned(t *testing.T) {
	cluster := newTestCluster()
	cluster.RetryDown = 2
	node := healthyNode("a:3306", 101, 1)
	node.Processed = false
	node.ProxyStatus = "SHUNNED"
	cluster.ReaderNodes["a:3306"] = node

	cluster.evaluateAllProcessedNodes()
	got, ok := cluster.ActionNodes["101_a:3306"]
	if !ok || got.ActionType != node.MOVE_DOWN_HG_CHANGE() || got.RetryDown != 1 {
		t.Errorf("ActionNodes = %v", cluster.ActionNodes)
	}
}

func TestEvaluateAllProcessedNodes_AlignsBackup(t *testing.T) {
	cluster := newTestCluster()
	node := healthyNode("a:3306", 101, 1)
	cluster.ReaderNodes["a:3306"] = node
	cluster.BackupReaders["a:3306"] = DataNodeImpl{Dns: "a:3306", HostgroupId: 8101, Weight: 9}
	cluster.BackupWriters["a:3306"] = DataNodeImpl{Dns: "a:3306", HostgroupId: 8100, Weight: 9}

	cluster.evaluateAllProcessedNodes()
	for name, bk := range map[string]DataNodeImpl{"reader": cluster.BackupReaders["a:3306"], "writer": cluster.BackupWriters["a:3306"]} {
		if bk.WsrepStatus != 4 || bk.Weight != 9 || bk.HostgroupId < 8100 {
			t.Errorf("backup %s not aligned: %+v", name, bk)
		}
	}
}

func TestCheckMissingForNodes(t *testing.T) {
	cluster := newTestCluster()
	cluster.BackupReaders["a"] = DataNodeImpl{Dns: "a", HostgroupId: 8101}
	cluster.BackupWriters["b"] = DataNodeImpl{Dns: "b", HostgroupId: 8100}
	eval := map[string]DataNodeImpl{"a": {Dns: "a", HostgroupId: 101}}

	got := cluster.checkMissingForNodes(eval)
	if got["a"].NodeIsNew {
		t.Error("existing node marked new")
	}
	if b := got["b"]; !b.NodeIsNew || b.HostgroupId != cluster.HgReaderId {
		t.Errorf("missing node not added as new reader: %+v", b)
	}
}

func TestCheckFailoverIfFound(t *testing.T) {
	cluster := newTestCluster()
	if !cluster.checkFailoverIfFound() {
		t.Error("no failover required: want true")
	}
	cluster.RequireFailover = true
	if cluster.checkFailoverIfFound() {
		t.Error("failover required, no writers, no candidate: want false")
	}
	cluster.FailOverNode = DataNodeImpl{HostgroupId: 8100}
	if !cluster.checkFailoverIfFound() {
		t.Error("failover candidate present: want true")
	}
}

func TestCheckActiveFailover(t *testing.T) {
	cluster := newTestCluster()
	cluster.ActiveFailover = 0
	if cluster.checkActiveFailover() {
		t.Error("ActiveFailover=0: want false")
	}
	cluster.ActiveFailover = 1
	if !cluster.checkActiveFailover() {
		t.Error("ActiveFailover=1: want true")
	}
}

// ****************  cleanup / writers **********************************

func TestCleanUpForLeftOver(t *testing.T) {
	cluster := newTestCluster()
	w := healthyNode("a:3306", 100, 1)
	r := healthyNode("b:3306", 101, 1)
	cluster.WriterNodes["a:3306"] = w
	cluster.ReaderNodes["b:3306"] = r
	cluster.OffLineWriters["a:3306"] = w
	cluster.OffLineReaders["b:3306"] = r

	cluster.cleanUpForLeftOver()
	if got := cluster.ActionNodes["5000_9100_a:3306"]; got.ActionType != w.DELETE_NODE() || got.HostgroupId != 9100 {
		t.Errorf("writer left over not scheduled for delete: %v", cluster.ActionNodes)
	}
	if got := cluster.ActionNodes["5000_9101_b:3306"]; got.ActionType != r.DELETE_NODE() || got.HostgroupId != 9101 {
		t.Errorf("reader left over not scheduled for delete: %v", cluster.ActionNodes)
	}
	if len(cluster.OffLineWriters) != 0 || len(cluster.OffLineReaders) != 0 {
		t.Error("offline maps not cleaned")
	}
}

func TestCleanUpForLeftOver_PersistPrimaryResetsDefaults(t *testing.T) {
	cluster := newTestCluster()
	cluster.PersistPrimarySettings = 1
	cluster.OffLineWriters["a"] = DataNodeImpl{Dns: "a", HostgroupId: 9100, Weight: 1}
	cluster.BackupWriters["a"] = DataNodeImpl{Dns: "a", HostgroupId: 8100, Weight: 10}
	cluster.OffLineReaders["b"] = DataNodeImpl{Dns: "b", HostgroupId: 9101, Weight: 5}
	cluster.BackupReaders["b"] = DataNodeImpl{Dns: "b", HostgroupId: 8101, Weight: 5}

	cluster.cleanUpForLeftOver()
	got, ok := cluster.ActionNodes["2010_9100_a"]
	if !ok || got.Weight != 10 || got.ActionType != got.RESET_DEFAULTS() {
		t.Errorf("RESET_DEFAULTS not scheduled for writer: %v", cluster.ActionNodes)
	}
	if len(cluster.ActionNodes) != 1 {
		t.Errorf("unexpected actions for node already at defaults: %v", cluster.ActionNodes)
	}
}

func TestCleanWriters_RespectManualOfflineSoft(t *testing.T) {
	cluster := newTestCluster()
	cluster.ActiveFailover = 1
	cluster.config.Proxysql.RespectManualOfflineSoft = true
	w := healthyNode("a:3306", 100, 1)
	w.ProxyStatus = "OFFLINE_SOFT"
	cluster.WriterNodes["a:3306"] = w
	cluster.BackupWriters["a:3306"] = w
	cluster.ActionNodes["100_a:3306"] = w

	if !cluster.cleanWriters() {
		t.Fatal("cleanWriters() = false")
	}
	if len(cluster.WriterNodes) != 0 || len(cluster.BackupWriters) != 0 || len(cluster.ActionNodes) != 0 {
		t.Errorf("writer=%v backup=%v actions=%v", cluster.WriterNodes, cluster.BackupWriters, cluster.ActionNodes)
	}
	if !cluster.RequireFailover || cluster.HasPrimary {
		t.Errorf("RequireFailover=%v HasPrimary=%v", cluster.RequireFailover, cluster.HasPrimary)
	}
}

func TestProcessDownActionMap(t *testing.T) {
	t.Run("last writer going down requires failover", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.ActiveFailover = 1
		w := healthyNode("a:3306", 100, 1)
		cluster.WriterNodes["a:3306"] = w
		cluster.BackupWriters["a:3306"] = w
		w.ActionType = w.MOVE_DOWN_OFFLINE()
		cluster.ActionNodes["100_a:3306"] = w

		cluster.processDownActionMap()
		if !cluster.RequireFailover || cluster.HasPrimary || cluster.Haswriter || len(cluster.WriterNodes) != 0 || len(cluster.BackupWriters) != 0 {
			t.Errorf("cluster = RequireFailover:%v HasPrimary:%v Haswriter:%v writers:%v", cluster.RequireFailover, cluster.HasPrimary, cluster.Haswriter, cluster.WriterNodes)
		}
	})
	t.Run("active failover disabled", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.ActiveFailover = 0
		w := healthyNode("a:3306", 100, 1)
		cluster.WriterNodes["a:3306"] = w
		w.ActionType = w.MOVE_DOWN_HG_CHANGE()
		cluster.ActionNodes["100_a:3306"] = w

		cluster.processDownActionMap()
		if cluster.RequireFailover || len(cluster.WriterNodes) != 0 {
			t.Errorf("RequireFailover=%v writers=%v", cluster.RequireFailover, cluster.WriterNodes)
		}
	})
	t.Run("one of many writers going down", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.ActiveFailover = 1
		a := healthyNode("a:3306", 100, 1)
		b := healthyNode("b:3306", 100, 1)
		cluster.WriterNodes["a:3306"] = a
		cluster.WriterNodes["b:3306"] = b
		a.ActionType = a.MOVE_DOWN_OFFLINE()
		cluster.ActionNodes["100_a:3306"] = a

		cluster.processDownActionMap()
		if cluster.RequireFailover || len(cluster.WriterNodes) != 1 {
			t.Errorf("RequireFailover=%v writers=%v", cluster.RequireFailover, cluster.WriterNodes)
		}
	})
	t.Run("retry not reached", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.RetryDown = 3
		w := healthyNode("a:3306", 100, 1)
		cluster.WriterNodes["a:3306"] = w
		w.ActionType = w.MOVE_DOWN_OFFLINE()
		w.RetryDown = 1
		cluster.ActionNodes["100_a:3306"] = w

		cluster.processDownActionMap()
		if len(cluster.WriterNodes) != 1 {
			t.Errorf("writer removed before retries exhausted")
		}
	})
}

func TestProcessUpActionMap(t *testing.T) {
	up := func(dns string, weight int) DataNodeImpl {
		n := healthyNode(dns, 100, weight)
		n.ActionType = n.MOVE_UP_OFFLINE()
		return n
	}

	t.Run("failover picks candidate", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.RequireFailover = true
		cluster.HasPrimary = false
		b := up("b:3306", 5)
		cluster.ActionNodes["100_b:3306"] = b

		cluster.processUpActionMap()
		if cluster.FailOverNode.Dns != "b:3306" {
			t.Errorf("FailOverNode = %q", cluster.FailOverNode.Dns)
		}
		if _, ok := cluster.WriterNodes["b:3306"]; !ok {
			t.Error("failover candidate not in writers")
		}
	})
	t.Run("free writer slot promotes node", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.ActionNodes["100_b:3306"] = up("b:3306", 5)
		cluster.processUpActionMap()
		if _, ok := cluster.WriterNodes["b:3306"]; !ok {
			t.Error("node not promoted to writer")
		}
	})
	t.Run("enough writers drops action", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.WriterNodes["a:3306"] = healthyNode("a:3306", 100, 10)
		cluster.ActionNodes["100_b:3306"] = up("b:3306", 5)
		cluster.processUpActionMap()
		if _, ok := cluster.ActionNodes["100_b:3306"]; ok {
			t.Error("action kept although writer HG is full")
		}
		if _, ok := cluster.WriterNodes["b:3306"]; ok {
			t.Error("node promoted although writer HG is full")
		}
	})
	t.Run("failback replaces lower weight writer", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.FailBack = true
		cluster.WriterNodes["a:3306"] = healthyNode("a:3306", 100, 1)
		b := up("b:3306", 10)
		cluster.BackupWriters["b:3306"] = b
		cluster.ActionNodes["100_b:3306"] = b

		cluster.processUpActionMap()
		if _, ok := cluster.WriterNodes["b:3306"]; !ok {
			t.Error("higher weight node not promoted on failback")
		}
		if _, ok := cluster.WriterNodes["a:3306"]; ok {
			t.Error("lower weight writer not removed on failback")
		}
		if got := cluster.ActionNodes["100_a:3306"]; got.ActionType != got.MOVE_DOWN_OFFLINE() {
			t.Errorf("lower writer not moved down: %v", cluster.ActionNodes)
		}
	})
	t.Run("failback keeps higher weight writer", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.FailBack = true
		cluster.WriterNodes["a:3306"] = healthyNode("a:3306", 100, 10)
		b := up("b:3306", 1)
		cluster.BackupWriters["b:3306"] = b
		cluster.ActionNodes["100_b:3306"] = b

		cluster.processUpActionMap()
		if _, ok := cluster.WriterNodes["a:3306"]; !ok {
			t.Error("higher weight writer removed")
		}
		if _, ok := cluster.ActionNodes["100_b:3306"]; ok {
			t.Error("lower weight node action not dropped")
		}
	})
	t.Run("retry not reached", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.RetryUp = 3
		cluster.ActionNodes["100_b:3306"] = up("b:3306", 5)
		cluster.processUpActionMap()
		if len(cluster.WriterNodes) != 0 {
			t.Error("node promoted before retries reached")
		}
	})
}

func TestProcessFailoverFailBack(t *testing.T) {
	t.Run("highest weight backup becomes failover node", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.RequireFailover = true
		addNode(cluster, "b:3306", 999, false)
		addNode(cluster, "c:3306", 998, false)

		cluster.processFailoverFailBack(cluster.BackupWriters)
		if cluster.FailOverNode.Dns != "b:3306" || cluster.FailOverNode.ActionType != cluster.FailOverNode.INSERT_WRITE() {
			t.Errorf("FailOverNode = %+v", cluster.FailOverNode)
		}
	})
	t.Run("node outside main segment ignored", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.RequireFailover = true
		cluster.MainSegment = 2
		addNode(cluster, "b:3306", 999, false)

		cluster.processFailoverFailBack(cluster.BackupWriters)
		if cluster.FailOverNode.Dns != "" {
			t.Errorf("FailOverNode = %q, want none", cluster.FailOverNode.Dns)
		}
	})
	t.Run("multi writer adds missing writer", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.MaxNumWriters = 2
		addNode(cluster, "a:3306", 10, true)
		addNode(cluster, "b:3306", 5, false)

		cluster.processFailoverFailBack(map[string]DataNodeImpl{"b:3306": cluster.BackupWriters["b:3306"]})
		if _, ok := cluster.WriterNodes["b:3306"]; !ok {
			t.Error("second writer not added")
		}
		if got := cluster.ActionNodes["4020_100_b:3306"]; got.ActionType != got.INSERT_WRITE() {
			t.Errorf("ActionNodes = %v", cluster.ActionNodes)
		}
		if _, ok := cluster.ActionNodes["5000_100_b:3306"]; !ok {
			t.Errorf("ActionNodes missing delete: %v", cluster.ActionNodes)
		}
	})
	t.Run("read only node not considered", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.RequireFailover = true
		b := addNode(cluster, "b:3306", 999, false)
		b.ReadOnly = true
		cluster.NodesPxc.Store("b:3306", b)

		cluster.processFailoverFailBack(cluster.BackupWriters)
		if cluster.FailOverNode.Dns != "" {
			t.Errorf("FailOverNode = %q, want none", cluster.FailOverNode.Dns)
		}
	})
}

func TestCopyPrimarySettingsToFailover(t *testing.T) {
	cluster := newTestCluster()
	cluster.PersistPrimarySettings = 2
	cluster.PersistPrimary[0] = DataNodeImpl{Dns: "a", Weight: 1000, Compression: 1, MaxConnection: 50, MaxReplicationLag: 3, MaxLatency: 4}
	cluster.BackupReaders["a"] = DataNodeImpl{Dns: "a", Weight: 777, MaxConnection: 60}
	cluster.ReaderNodes["b"] = DataNodeImpl{Dns: "b", HostgroupId: 101, Weight: 1}
	cluster.FailOverNode = DataNodeImpl{Dns: "b", HostgroupId: 8100, Weight: 1, ProxyStatus: "OFFLINE_SOFT"}
	cluster.ActionNodes["9100_b"] = DataNodeImpl{Dns: "b"}
	cluster.ActionNodes["9101_b"] = DataNodeImpl{Dns: "b"}

	cluster.copyPrimarySettingsToFailover()
	f := cluster.FailOverNode
	if f.Weight != 1000 || f.Compression != 1 || f.MaxConnection != 50 || f.MaxReplicationLag != 3 || f.MaxLatency != 4 ||
		f.ActionType != f.INSERT_WRITE() || f.ProxyStatus != "ONLINE" {
		t.Errorf("FailOverNode = %+v", f)
	}
	if _, ok := cluster.ActionNodes["9100_b"]; ok {
		t.Error("maintenance writer action not removed")
	}
	if _, ok := cluster.ActionNodes["9101_b"]; ok {
		t.Error("maintenance reader action not removed")
	}
	r, ok := cluster.ActionNodes["101_b"]
	if !ok || r.Weight != 777 || r.MaxConnection != 60 || r.ActionType != r.INSERT_READ() {
		t.Errorf("reader action = %+v, %v", r, ok)
	}
}

// ****************  readers **********************************

func TestPushNewNode(t *testing.T) {
	cluster := newTestCluster()
	node := DataNodeImpl{Dns: "a", HostgroupId: 101}
	if !cluster.pushNewNode(node) {
		t.Fatal("pushNewNode() = false")
	}
	del := cluster.ActionNodes["5000_9101_a"]
	ins := cluster.ActionNodes["4010_9101_a"]
	if del.ActionType != del.DELETE_NODE() || ins.ActionType != ins.INSERT_READ() || !ins.NodeIsNew || ins.HostgroupId != 9101 {
		t.Errorf("ActionNodes = %v", cluster.ActionNodes)
	}

	cluster.OffLineReaders["b"] = DataNodeImpl{Dns: "b"}
	if cluster.pushNewNode(DataNodeImpl{Dns: "b"}) {
		t.Error("pushNewNode() = true for node already in maintenance")
	}
}

func TestEvaluateReaders_WriterBecomesReaderWhenNoReaders(t *testing.T) {
	cluster := newTestCluster()
	w := healthyNode("a:3306", 100, 1)
	cluster.WriterNodes["a:3306"] = w

	cluster.evaluateReaders()
	if _, ok := cluster.ActionNodes["4010_9101_a:3306"]; !ok {
		t.Errorf("writer not pushed as reader: %v", cluster.ActionNodes)
	}
}

func TestEvaluateReaders_OfflineSoftReaderDroppedWithRespectManual(t *testing.T) {
	cluster := newTestCluster()
	cluster.config.Proxysql.RespectManualOfflineSoft = true
	addNode(cluster, "a:3306", 1, true)
	r := cluster.ReaderNodes["a:3306"]
	r.ProxyStatus = "OFFLINE_SOFT"
	cluster.ReaderNodes["a:3306"] = r

	cluster.evaluateReaders()
	if _, ok := cluster.BackupReaders["a:3306"]; ok {
		t.Error("backup reader kept although RespectManualOfflineSoft is set")
	}
}

// ****************  end to end **********************************

func TestGetActionList_HealthyClusterNoActions(t *testing.T) {
	cluster := newTestCluster()
	addNode(cluster, "a:3306", 1000, true)
	addNode(cluster, "b:3306", 999, false)
	addNode(cluster, "c:3306", 998, false)

	actions := cluster.GetActionList()
	if len(actions) != 0 {
		t.Errorf("GetActionList() = %v, want none", actions)
	}
	if !cluster.HasPrimary {
		t.Error("HasPrimary = false")
	}
}

func TestGetActionList_WriterInMaintenanceFailsOver(t *testing.T) {
	cluster := newTestCluster()
	cluster.ActiveFailover = 1
	a := addNode(cluster, "a:3306", 1000, true)
	addNode(cluster, "b:3306", 999, false)
	addNode(cluster, "c:3306", 998, false)

	// node A goes into maintenance in all its role maps
	a.Variables = map[string]string{}
	for k, v := range healthyNode("a:3306", 0, 0).Variables {
		a.Variables[k] = v
	}
	a.Variables["pxc_maint_mode"] = "MAINTENANCE"
	cluster.NodesPxc.Store("a:3306", a)
	cluster.consolidateNodes()

	actions := cluster.GetActionList()

	if got := actions["100_a:3306"]; got.ActionType != got.MOVE_DOWN_OFFLINE() {
		t.Errorf("writer not moved down: %v", actions)
	}
	if got := actions["101_a:3306"]; got.ActionType != got.MOVE_DOWN_OFFLINE() {
		t.Errorf("reader copy not moved down: %v", actions)
	}
	if !cluster.RequireFailover {
		t.Error("RequireFailover = false")
	}
	if cluster.FailOverNode.Dns != "b:3306" {
		t.Errorf("FailOverNode = %q, want b:3306 (highest weight)", cluster.FailOverNode.Dns)
	}
	if got, ok := actions["100_b:3306"]; !ok || got.ActionType != got.INSERT_WRITE() {
		t.Errorf("failover insert missing: %v", actions)
	}
}

func TestGetActionList_PersistPrimaryCopiesSettings(t *testing.T) {
	cluster := newTestCluster()
	cluster.ActiveFailover = 1
	cluster.PersistPrimarySettings = 1
	cluster.SinglePrimary = true
	a := addNode(cluster, "a:3306", 1000, true)
	addNode(cluster, "b:3306", 999, false)
	cluster.PersistPrimary[0] = cluster.WriterNodes["a:3306"]

	a.Variables["pxc_maint_mode"] = "MAINTENANCE"
	cluster.NodesPxc.Store("a:3306", a)
	cluster.consolidateNodes()

	actions := cluster.GetActionList()
	if got := actions["100_b:3306"]; got.Weight != 1000 || got.ProxyStatus != "ONLINE" {
		t.Errorf("failover node did not inherit primary settings: %+v", got)
	}
}

// ****************  connection handling (no server needed) **********************************

func TestCloseConnection_Nil(t *testing.T) {
	node := DataNodeImpl{}
	if node.CloseConnection() {
		t.Error("CloseConnection() on nil connection = true")
	}
}

func TestGetConnection_Failures(t *testing.T) {
	t.Run("missing CA file", func(t *testing.T) {
		node := DataNodeImpl{UseSsl: true, Ssl: &SslCertificates{sslCertificatePath: t.TempDir(), sslCa: "ca.pem"}}
		if node.GetConnection() {
			t.Error("GetConnection() = true")
		}
	})
	t.Run("invalid CA content", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "ca.pem"), []byte("not a cert"), 0600); err != nil {
			t.Fatal(err)
		}
		node := DataNodeImpl{UseSsl: true, Ssl: &SslCertificates{sslCertificatePath: dir, sslCa: "ca.pem"}}
		if node.GetConnection() {
			t.Error("GetConnection() = true")
		}
	})
	t.Run("unreachable server", func(t *testing.T) {
		node := DataNodeImpl{Ip: "127.0.0.1", Port: 1, User: "u", Password: "p", PingTimeout: 500}
		if node.GetConnection() {
			t.Error("GetConnection() = true")
		}
		if !node.NodeTCPDown {
			t.Error("NodeTCPDown = false")
		}
		if !node.CloseConnection() {
			t.Error("CloseConnection() = false on opened pool")
		}
	})
	t.Run("ssl without certificates object", func(t *testing.T) {
		node := DataNodeImpl{Ip: "127.0.0.1", Port: 1, UseSsl: true, PingTimeout: 500}
		if node.GetConnection() {
			t.Error("GetConnection() = true")
		}
	})
}

func TestGetInfo_UnreachableNode(t *testing.T) {
	cluster := newTestCluster()
	node := DataNodeImpl{Dns: "127.0.0.1:1", Ip: "127.0.0.1", Port: 1, Processed: true, PingTimeout: 500}
	var wg global.MyWaitGroup
	wg.IncreaseCounter()

	node.getInfo(&wg, cluster)
	stored, ok := cluster.NodesPxc.Load("127.0.0.1:1")
	if !ok || stored.Processed || !stored.NodeTCPDown {
		t.Errorf("stored node = %+v, %v", stored, ok)
	}
	if wg.ReportCounter() != 0 {
		t.Errorf("wait group counter = %d", wg.ReportCounter())
	}
}

// ****************  property-based tests (hegel) **********************************

// Retry counters written into a node comment are read back exactly, and the
// rest of the comment is preserved.
func TestProperty_GetRetry_RoundTrip(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		writeHG := hegel.Draw(ht, hegel.Integers(0, 100000))
		readHG := hegel.Draw(ht, hegel.Integers(0, 100000))
		up := hegel.Draw(ht, hegel.Integers(0, 1000))
		down := hegel.Draw(ht, hegel.Integers(0, 1000))
		// '=' and ';' are excluded only so the surrounding text cannot itself form a retry token
		pre := hegel.Draw(ht, hegel.Text().ExcludeCharacters("=;").MaxSize(30))
		post := hegel.Draw(ht, hegel.Text().ExcludeCharacters("=;").MaxSize(30))

		prefix := strconv.Itoa(writeHG) + "_W_" + strconv.Itoa(readHG) + "_R_retry_"
		node := DataNodeImpl{Comment: pre + prefix + "up=" + strconv.Itoa(up) + ";" + prefix + "down=" + strconv.Itoa(down) + ";" + post}
		node.getRetry(writeHG, readHG)

		if node.RetryUp != up || node.RetryDown != down {
			ht.Fatalf("getRetry() up=%d down=%d, want %d/%d", node.RetryUp, node.RetryDown, up, down)
		}
		if node.Comment != pre+post {
			ht.Fatalf("getRetry() comment = %q, want %q", node.Comment, pre+post)
		}
	}, hegel.WithTestCases(1000))
}

// Retry tokens that belong to a different hostgroup pair are ignored and left in place.
func TestProperty_GetRetry_OtherHostgroupsUntouched(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		writeHG := hegel.Draw(ht, hegel.Integers(0, 100000))
		readHG := hegel.Draw(ht, hegel.Integers(0, 100000))
		otherW := hegel.Draw(ht, hegel.Integers(0, 100000))
		otherR := hegel.Draw(ht, hegel.Integers(0, 100000))
		ht.Assume(otherW != writeHG || otherR != readHG)
		retry := hegel.Draw(ht, hegel.Integers(0, 1000))

		comment := strconv.Itoa(otherW) + "_W_" + strconv.Itoa(otherR) + "_R_retry_up=" + strconv.Itoa(retry) + ";"
		node := DataNodeImpl{Comment: comment}
		node.getRetry(writeHG, readHG)

		if node.RetryUp != 0 || node.RetryDown != 0 || node.Comment != comment {
			ht.Fatalf("getRetry(%d, %d) changed node for foreign token %q: %+v", writeHG, readHG, comment, node)
		}
	}, hegel.WithTestCases(1000))
}

// ****************  stateful property tests: random clusters through GetActionList **********************************

type nodeSpec struct {
	weight      int
	segment     int
	localState  string
	readOnly    bool
	reject      bool
	maint       bool
	primary     bool
	processed   bool
	proxyStatus string
	writer      bool
	reader      bool
}

type clusterSpec struct {
	nodes          []nodeSpec
	maxNumWriters  int
	failBack       bool
	activeFailover int
	writerIsReader int
}

// drawNode draws one node; healthy draws every fault off and keeps it ONLINE and processed.
func drawNode(ht *hegel.T, healthy bool) nodeSpec {
	n := nodeSpec{
		weight:      hegel.Draw(ht, hegel.Integers(1, 1000)),
		segment:     hegel.Draw(ht, hegel.SampledFrom([]int{0, 0, 0, 1})),
		localState:  "4",
		primary:     true,
		processed:   true,
		proxyStatus: "ONLINE",
		reader:      true,
	}
	if !healthy {
		n.localState = hegel.Draw(ht, hegel.SampledFrom([]string{"4", "4", "4", "2", "1", "3"}))
		n.readOnly = hegel.Draw(ht, hegel.WeightedBooleans(0.2))
		n.reject = hegel.Draw(ht, hegel.WeightedBooleans(0.2))
		n.maint = hegel.Draw(ht, hegel.WeightedBooleans(0.2))
		n.primary = hegel.Draw(ht, hegel.WeightedBooleans(0.85))
		n.processed = hegel.Draw(ht, hegel.WeightedBooleans(0.9))
		n.proxyStatus = hegel.Draw(ht, hegel.SampledFrom([]string{"ONLINE", "ONLINE", "ONLINE", "OFFLINE_SOFT", "SHUNNED"}))
		n.reader = hegel.Draw(ht, hegel.WeightedBooleans(0.85))
	}
	return n
}

func drawClusterSpec(ht *hegel.T, healthy bool) clusterSpec {
	size := hegel.Draw(ht, hegel.Integers(1, 5))
	spec := clusterSpec{
		maxNumWriters:  hegel.Draw(ht, hegel.Integers(1, 2)),
		failBack:       hegel.Draw(ht, hegel.Booleans()),
		activeFailover: hegel.Draw(ht, hegel.Integers(0, 2)),
		writerIsReader: hegel.Draw(ht, hegel.Integers(0, 1)),
	}
	// SanityCheck rejects writerIsAlsoReader != 1 together with more than one writer
	if spec.maxNumWriters > 1 {
		spec.writerIsReader = 1
	}
	for i := 0; i < size; i++ {
		spec.nodes = append(spec.nodes, drawNode(ht, healthy))
	}
	// writers: a random subset, at most maxNumWriters+1 so over-provisioned clusters are covered too
	for i := range spec.nodes {
		spec.nodes[i].writer = hegel.Draw(ht, hegel.WeightedBooleans(0.4))
	}
	return spec
}

// String renders a spec as one line per node for failure messages.
func (spec clusterSpec) String() string {
	out := fmt.Sprintf(" cluster: maxNumWriters=%d failBack=%v activeFailover=%d writerIsReader=%d\n",
		spec.maxNumWriters, spec.failBack, spec.activeFailover, spec.writerIsReader)
	for i, n := range spec.nodes {
		out += fmt.Sprintf(" %s weight=%d seg=%d state=%s primary=%v read_only=%v reject=%v maint=%v processed=%v proxy=%s writer=%v reader=%v\n",
			specDns(i), n.weight, n.segment, n.localState, n.primary, n.readOnly, n.reject, n.maint, n.processed, n.proxyStatus, n.writer, n.reader)
	}
	return out
}

func specDns(i int) string { return "127.0.0.1:" + strconv.Itoa(43306+i) }

// buildCluster turns a spec into a cluster the same way loadNodes/getNodesInfo/consolidateHGs would.
func buildCluster(spec clusterSpec) *DataClusterImpl {
	cluster := newTestCluster()
	cluster.MaxNumWriters = spec.maxNumWriters
	cluster.SinglePrimary = spec.maxNumWriters == 1
	cluster.FailBack = spec.failBack
	cluster.ActiveFailover = spec.activeFailover
	cluster.WriterIsReader = spec.writerIsReader
	cluster.MainSegment = 0
	for i, s := range spec.nodes {
		dns := specDns(i)
		node := healthyNode(dns, cluster.HgReaderId, s.weight)
		node.Variables["wsrep_provider_options"] = "gmcast.segment = " + strconv.Itoa(s.segment) + "; pc.weight = 1"
		node.Variables["read_only"] = map[bool]string{true: "ON", false: "OFF"}[s.readOnly]
		node.Variables["wsrep_reject_queries"] = map[bool]string{true: "ALL", false: "NONE"}[s.reject]
		node.Variables["pxc_maint_mode"] = map[bool]string{true: "MAINTENANCE", false: "DISABLED"}[s.maint]
		node.Status["wsrep_local_state"] = s.localState
		node.Status["wsrep_cluster_status"] = map[bool]string{true: "Primary", false: "non-Primary"}[s.primary]
		node.setParameters()
		node.Processed = s.processed
		node.ProxyStatus = s.proxyStatus
		cluster.NodesPxc.Store(dns, node)

		if s.reader {
			cluster.ReaderNodes[dns] = node
		}
		if s.writer {
			w := node
			w.HostgroupId = cluster.HgWriterId
			cluster.WriterNodes[dns] = w
		}
		bkW, bkR := node, node
		bkW.HostgroupId, bkR.HostgroupId = cluster.BakcupHgWriterId, cluster.BackupHgReaderId
		bkW.ProxyStatus, bkR.ProxyStatus = "ONLINE", "ONLINE"
		cluster.BackupWriters[dns] = bkW
		cluster.BackupReaders[dns] = bkR
	}
	cluster.consolidateHGs()
	return cluster
}

func actionSummary(actions map[string]DataNodeImpl) map[string]int {
	out := make(map[string]int, len(actions))
	for k, v := range actions {
		out[k] = v.ActionType
	}
	return out
}

// The same cluster state always produces the same actions and the same failover choice.
func TestProperty_GetActionList_Deterministic(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		spec := drawClusterSpec(ht, false)
		a, b := buildCluster(spec), buildCluster(spec)
		got1, got2 := actionSummary(a.GetActionList()), actionSummary(b.GetActionList())
		if !reflect.DeepEqual(got1, got2) || a.FailOverNode.Dns != b.FailOverNode.Dns {
			ht.Fatalf("same input, different results:\n run 1: %v failover=%q\n run 2: %v failover=%q\n%s", got1, a.FailOverNode.Dns, got2, b.FailOverNode.Dns, spec)
		}
	}, hegel.WithTestCases(1000))
}

// A node chosen for failover is one the scheduler itself would accept as a writer.
func TestProperty_GetActionList_FailoverNodeUsable(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		spec := drawClusterSpec(ht, false)
		cluster := buildCluster(spec)
		cluster.GetActionList()
		if !cluster.RequireFailover || cluster.FailOverNode.HostgroupId == 0 {
			return
		}
		live, _ := cluster.NodesPxc.Load(cluster.FailOverNode.Dns)
		if !live.Processed || live.ReadOnly || live.WsrepRejectqueries || live.PxcMaintMode != "DISABLED" ||
			live.WsrepClusterStatus != "Primary" || live.WsrepStatus != 4 {
			ht.Fatalf("failover node %s is not usable: processed=%v read_only=%v reject=%v maint=%s cluster=%s state=%d\n%s",
				live.Dns, live.Processed, live.ReadOnly, live.WsrepRejectqueries, live.PxcMaintMode, live.WsrepClusterStatus, live.WsrepStatus, spec)
		}
		if live.WsrepSegment != cluster.MainSegment && cluster.ActiveFailover <= 1 {
			ht.Fatalf("failover node %s is in segment %d, main segment %d, activeFailover=%d", live.Dns, live.WsrepSegment, cluster.MainSegment, cluster.ActiveFailover)
		}
	}, hegel.WithTestCases(1000))
}

// onlineWritersAfter replays the actions on the writer hostgroup the way ProcessChanges turns them
// into SQL (retries are 0 in these specs, so no action is deferred) and returns the nodes that end up
// ONLINE in it. Removals are applied before insertions, the order most favourable to the cap.
func onlineWritersAfter(spec clusterSpec, cluster *DataClusterImpl, actions map[string]DataNodeImpl) map[string]bool {
	w := cluster.HgWriterId
	m := cluster.MaintenanceHgRange
	online := map[string]bool{}
	for i, n := range spec.nodes {
		if n.writer && n.proxyStatus == "ONLINE" {
			online[specDns(i)] = true
		}
	}
	for _, a := range actions {
		switch a.ActionType {
		case 3001, 3010, 3020, 5000, 5101:
			if a.HostgroupId == w {
				delete(online, a.Dns)
			}
		case 4020, 5001:
			delete(online, a.Dns)
		}
	}
	for _, a := range actions {
		switch a.ActionType {
		case 1000, 3030, 2010:
			if a.HostgroupId == w {
				online[a.Dns] = true
			}
		case 1010:
			if a.HostgroupId-m == w {
				online[a.Dns] = true
			}
		case 4020, 5001:
			if !a.NodeIsNew && a.ProxyStatus == "ONLINE" {
				online[a.Dns] = true
			}
		}
	}
	return online
}

// After the actions are applied, the writer hostgroup never has more reachable ONLINE writers than
// maxNumWriters. Unreachable writers that ProxySQL still shows ONLINE are left alone until ProxySQL
// shuns them (evaluateAllProcessedNodes), so they are not counted.
func TestProperty_GetActionList_WriterCap(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		spec := drawClusterSpec(ht, false)
		cluster := buildCluster(spec)
		actions := cluster.GetActionList()
		var reachable []string
		for dns := range onlineWritersAfter(spec, cluster, actions) {
			if live, _ := cluster.NodesPxc.Load(dns); live.Processed {
				reachable = append(reachable, dns)
			}
		}
		sort.Strings(reachable)
		if len(reachable) > cluster.MaxNumWriters {
			ht.Fatalf("%d reachable ONLINE writers after applying the actions, maxNumWriters=%d: %v\n actions: %v\n%s",
				len(reachable), cluster.MaxNumWriters, reachable, actionSummary(actions), spec)
		}
	}, hegel.WithTestCases(1000))
}

// A healthy cluster whose roles already match the configuration needs no changes.
func TestProperty_GetActionList_HealthyClusterNoActions(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		spec := drawClusterSpec(ht, true)
		spec.failBack = false
		spec.writerIsReader = 1
		spec.maxNumWriters = 1
		for i := range spec.nodes {
			spec.nodes[i].writer = false
			spec.nodes[i].segment = 0
		}
		spec.nodes[hegel.Draw(ht, hegel.Integers(0, len(spec.nodes)-1))].writer = true
		cluster := buildCluster(spec)
		if actions := cluster.GetActionList(); len(actions) != 0 {
			ht.Fatalf("healthy cluster produced actions: %v", actionSummary(actions))
		}
	}, hegel.WithTestCases(1000))
}

// A retry token of writer HG 110 is not read as one of writer HG 10 (same reader HG).
func TestGetRetry_WriterHostgroupSuffix(t *testing.T) {
	comment := "110_W_11_R_retry_up=3;110_W_11_R_retry_down=4;"
	node := DataNodeImpl{Comment: comment}
	node.getRetry(10, 11)
	if node.RetryUp != 0 || node.RetryDown != 0 || node.Comment != comment {
		t.Errorf("getRetry(10, 11) = up %d down %d comment %q, want 0/0 and the comment unchanged", node.RetryUp, node.RetryDown, node.Comment)
	}
}

func keys(m map[string]DataNodeImpl) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
