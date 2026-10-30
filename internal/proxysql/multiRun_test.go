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

package proxysql

import (
	"database/sql"
	"fmt"
	"net"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	global "pxc_scheduler_handler/internal/global"
	"pxc_scheduler_handler/internal/testutil"

	"hegel.dev/go/hegel"
)

// ****************  multi-run simulation **********************************
//
// Each simulated scheduler run uses the real code for everything except talking to the PXC nodes:
// getParametersFromProxySQL, loadNodes (reading runtime_mysql_servers joined with
// stats_mysql_connection_pool), checkMissingForNodes, consolidateNodes/HGs, GetActionList and
// ProcessChanges. The node checks of getNodesInfo are replaced by injected wsrep states.
//
// ProxySQL itself is modelled in two ways: LOAD MYSQL SERVERS TO RUNTIME copies mysql_servers to
// runtime_mysql_servers before each run, and its monitor shows unreachable ONLINE servers as SHUNNED there.

type pxcState struct {
	weight    int
	reachable bool
	primary   bool
	synced    bool // wsrep_local_state 4, otherwise 1 (joining)
	readOnly  bool
	maint     bool
	reject    bool
}

func (s pxcState) String() string {
	return fmt.Sprintf("w=%d reach=%v primary=%v synced=%v ro=%v maint=%v reject=%v", s.weight, s.reachable, s.primary, s.synced, s.readOnly, s.maint, s.reject)
}

func healthyPxc(weight int) pxcState {
	return pxcState{weight: weight, reachable: true, primary: true, synced: true}
}

// readerOK: the node can serve reads. writerOK: it can also take writes.
func (s pxcState) readerOK() bool {
	return s.reachable && s.primary && s.synced && !s.maint && !s.reject
}
func (s pxcState) writerOK() bool { return s.readerOK() && !s.readOnly }

func simDns(i int) string { return net.JoinHostPort("10.0.2."+strconv.Itoa(i+1), "3306") }

type simCluster struct {
	db    *sql.DB
	rec   *testutil.AdminConnector
	cfg   global.Configuration
	nodes []pxcState
	run   int
	trace []string
}

func newSimCluster(t *testing.T, nodes []pxcState, retryUp, retryDown int, failBack bool) *simCluster {
	db, rec := newAdminDB(t)
	for _, ddl := range []string{
		strings.Replace(testutil.MysqlServersDDL, "mysql_servers", "runtime_mysql_servers", 1),
		`CREATE VIEW stats_mysql_connection_pool AS SELECT hostgroup_id AS hostgroup, hostname AS srv_host, port AS srv_port, 0 AS ConnUsed FROM runtime_mysql_servers`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	var cfg global.Configuration
	p := &cfg.Pxcluster
	p.ClusterId, p.HgW, p.HgR, p.BckHgW, p.BckHgR = 1, 100, 101, 8100, 8101
	p.ConfigHgRange, p.MaintenanceHgRange = 8000, 9000
	p.SinglePrimary, p.MaxNumWriters, p.WriterIsAlsoReader = true, 1, 1
	p.RetryUp, p.RetryDown, p.FailBack = retryUp, retryDown, failBack
	p.MainSegment, p.ActiveFailover, p.CheckTimeOut = 1, 1, 2000

	sim := &simCluster{db: db, rec: rec, cfg: cfg, nodes: nodes}

	// what an administrator configures: every node in the config hostgroups and as a reader,
	// the heaviest node as the writer
	top := 0
	for i, n := range nodes {
		if n.weight > nodes[top].weight {
			top = i
		}
	}
	for i, n := range nodes {
		host, portS, _ := net.SplitHostPort(simDns(i))
		port, _ := strconv.Atoi(portS)
		hgs := []int{8100, 8101, 101}
		if i == top {
			hgs = append(hgs, 100)
		}
		for _, hg := range hgs {
			if err := insertServer(db, serverRow{hg: hg, host: host, port: port, status: "ONLINE", weight: n.weight, maxConn: 1000}); err != nil {
				t.Fatal(err)
			}
		}
	}
	return sim
}

// loadRuntime does what LOAD MYSQL SERVERS TO RUNTIME and the ProxySQL monitor do.
func (s *simCluster) loadRuntime() error {
	if _, err := s.db.Exec("DELETE FROM runtime_mysql_servers"); err != nil {
		return err
	}
	if _, err := s.db.Exec("INSERT INTO runtime_mysql_servers SELECT * FROM mysql_servers"); err != nil {
		return err
	}
	for i, n := range s.nodes {
		if !n.reachable {
			host, portS, _ := net.SplitHostPort(simDns(i))
			if _, err := s.db.Exec("UPDATE runtime_mysql_servers SET status='SHUNNED' WHERE hostname=? AND port=? AND status='ONLINE'", host, portS); err != nil {
				return err
			}
		}
	}
	return nil
}

// probe stands in for getInfo: it fills the node from the simulated server's variables and status.
func (s *simCluster) probe(node DataNodeImpl) DataNodeImpl {
	idx := -1
	for i := range s.nodes {
		if simDns(i) == node.Dns {
			idx = i
		}
	}
	if idx < 0 || !s.nodes[idx].reachable {
		node.NodeTCPDown = true
		node.Processed = false
		return node
	}
	st := s.nodes[idx]
	onOff := map[bool]string{true: "ON", false: "OFF"}
	node.Variables = map[string]string{
		"pxc_maint_mode":                  map[bool]string{true: "MAINTENANCE", false: "DISABLED"}[st.maint],
		"wsrep_sst_donor_rejects_queries": "OFF",
		"wsrep_provider_options":          "gmcast.segment = 1; pc.weight = 1",
		"wsrep_cluster_name":              "sim",
		"wsrep_node_name":                 node.Dns,
		"wsrep_reject_queries":            map[bool]string{true: "ALL", false: "NONE"}[st.reject],
		"read_only":                       onOff[st.readOnly],
		"server_uuid":                     "uuid-" + node.Dns,
	}
	node.Status = map[string]string{
		"wsrep_connected":        "ON",
		"wsrep_desync_count":     "0",
		"wsrep_gcomm_uuid":       "gcomm-" + node.Dns,
		"wsrep_cluster_status":   map[bool]string{true: "Primary", false: "non-Primary"}[st.primary],
		"wsrep_local_recv_queue": "0",
		"wsrep_cluster_size":     strconv.Itoa(len(s.nodes)),
		"wsrep_ready":            "ON",
		"wsrep_local_state":      map[bool]string{true: "4", false: "1"}[st.synced],
	}
	node.Processed = true
	setParameters(&node)
	return node
}

// step runs the scheduler once and reports whether ProcessChanges succeeded.
func (s *simCluster) step() (bool, error) {
	s.run++
	if err := s.loadRuntime(); err != nil {
		return false, err
	}
	cfg := s.cfg
	cluster := new(DataClusterImpl)
	cluster.ClusterIdentifier = cfg.Pxcluster.ClusterId
	cluster.CheckTimeout = cfg.Pxcluster.CheckTimeOut
	cluster.MainSegment = cfg.Pxcluster.MainSegment
	cluster.ActiveFailover = cfg.Pxcluster.ActiveFailover
	cluster.FailBack = cfg.Pxcluster.FailBack
	getParametersFromProxySQL(cluster, cfg)
	if err := loadNodes(cluster, cfg, s.db); err != nil {
		return false, fmt.Errorf("loading the cluster failed: %w", err)
	}
	checkMissingForNodes(cluster)
	for dns, node := range cluster.NodesPxc.ExposeMap() {
		cluster.NodesPxc.Store(dns, s.probe(node))
	}
	consolidateNodes(cluster)
	consolidateHGs(cluster)
	actions := cluster.GetActionList()
	proxy := &ProxySQLNodeImpl{Connection: s.db, MySQLCluster: cluster, Config: &cfg, ActionNodeList: actions}
	s.rec.Reset()
	ok := proxy.ProcessChanges()
	var acts []string
	for k, a := range actions {
		acts = append(acts, k+":"+strconv.Itoa(a.ActionType))
	}
	sort.Strings(acts)
	s.trace = append(s.trace, fmt.Sprintf("run %d: ok=%v actions=%v", s.run, ok, acts))
	return ok, nil
}

// online returns the nodes ONLINE in a hostgroup as ProxySQL serves it after the last run.
func (s *simCluster) online(hg int) (map[int]bool, error) {
	if err := s.loadRuntime(); err != nil {
		return nil, err
	}
	rows, err := s.db.Query("SELECT hostname, port FROM runtime_mysql_servers WHERE hostgroup_id=? AND status='ONLINE'", hg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int]bool{}
	for rows.Next() {
		var host string
		var port int
		if err := rows.Scan(&host, &port); err != nil {
			return nil, err
		}
		for i := range s.nodes {
			if simDns(i) == net.JoinHostPort(host, strconv.Itoa(port)) {
				out[i] = true
			}
		}
	}
	return out, rows.Err()
}

// layout is mysql_servers without the comment column, for comparing runs.
func (s *simCluster) layout() ([]string, error) {
	rows, err := readServers(s.db)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, r := range rows {
		out = append(out, fmt.Sprintf("%d %s %s w=%d", r.hg, r.key(), r.status, r.weight))
	}
	return out, nil
}

func (s *simCluster) describe() string {
	var b strings.Builder
	fmt.Fprintf(&b, " retryUp=%d retryDown=%d failBack=%v\n", s.cfg.Pxcluster.RetryUp, s.cfg.Pxcluster.RetryDown, s.cfg.Pxcluster.FailBack)
	for i, n := range s.nodes {
		fmt.Fprintf(&b, " node %d %s: %s\n", i, simDns(i), n)
	}
	rows, _ := s.layout()
	b.WriteString(" mysql_servers:\n  " + strings.Join(rows, "\n  ") + "\n")
	b.WriteString(" " + strings.Join(s.trace, "\n "))
	return b.String()
}

// checkRun checks what must hold after every run: the changes applied, and with single primary
// there is never more than one reachable ONLINE writer.
func (s *simCluster) checkRun(ht *hegel.T, ok bool, err error) {
	if err != nil {
		ht.Fatalf("run %d: %v\n%s", s.run, err, s.describe())
	}
	if !ok {
		ht.Fatalf("run %d: ProcessChanges failed\n statements:\n  %s\n%s", s.run, strings.Join(s.rec.Statements(), "\n  "), s.describe())
	}
	writers, err := s.online(100)
	if err != nil {
		ht.Fatalf("read: %v", err)
	}
	if max := s.cfg.Pxcluster.MaxNumWriters; len(writers) > max {
		ht.Fatalf("run %d: %d ONLINE writers %v with maxNumWriters=%d\n%s", s.run, len(writers), writers, max, s.describe())
	}
}

func drawPxcFault(ht *hegel.T, n pxcState) pxcState {
	switch hegel.Draw(ht, hegel.SampledFrom([]string{"heal", "heal", "unreachable", "nonPrimary", "joining", "readOnly", "maint", "reject"})) {
	case "heal":
		return healthyPxc(n.weight)
	case "unreachable":
		n.reachable = false
	case "nonPrimary":
		n.primary = false
	case "joining":
		n.synced = false
	case "readOnly":
		n.readOnly = true
	case "maint":
		n.maint = true
	case "reject":
		n.reject = true
	}
	return n
}

// After any sequence of faults and repairs, once the nodes stop changing the scheduler settles
// within a bounded number of runs on the layout the configuration asks for:
//   - mysql_servers stops changing;
//   - as many ONLINE writers as maxNumWriters allows, all able to take writes (with single primary and
//     failBack, the heaviest such node);
//   - every node that can serve reads is an ONLINE reader, and no other node is ONLINE as reader or writer.
//
// After every run, there are never more ONLINE writers than maxNumWriters.
//
// This test covers single primary with or without failBack and two writers without failBack;
// TestProperty_MultiRun_SettlesMultiWriterFailBack covers two writers with failBack.
func TestProperty_MultiRun_Settles(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		maxWriters := hegel.Draw(ht, hegel.SampledFrom([]int{1, 1, 2}))
		failBack := maxWriters == 1 && hegel.Draw(ht, hegel.Booleans())
		checkSettles(t, ht, maxWriters, failBack)
	}, hegel.WithTestCases(simCases(200)))
}

func TestProperty_MultiRun_SettlesMultiWriterFailBack(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		checkSettles(t, ht, 2, true)
	}, hegel.WithTestCases(simCases(100)))
}

func checkSettles(t *testing.T, ht *hegel.T, maxWriters int, failBack bool) {
	n := hegel.Draw(ht, hegel.Integers(2, 4))
	nodes := make([]pxcState, n)
	for i := range nodes {
		nodes[i] = healthyPxc(hegel.Draw(ht, hegel.Integers(1, 1000)))
	}
	retryUp := hegel.Draw(ht, hegel.Integers(0, 3))
	retryDown := hegel.Draw(ht, hegel.Integers(0, 3))
	sim := newSimCluster(t, nodes, retryUp, retryDown, failBack)
	sim.cfg.Pxcluster.MaxNumWriters = maxWriters
	sim.cfg.Pxcluster.SinglePrimary = maxWriters == 1

	phases := hegel.Draw(ht, hegel.Integers(1, 4))
	for p := 0; p < phases; p++ {
		i := hegel.Draw(ht, hegel.Integers(0, n-1))
		sim.nodes[i] = drawPxcFault(ht, sim.nodes[i])
		sim.trace = append(sim.trace, fmt.Sprintf("node %d now %s", i, sim.nodes[i]))
		for r := hegel.Draw(ht, hegel.Integers(1, 3)); r > 0; r-- {
			ok, err := sim.step()
			sim.checkRun(ht, ok, err)
		}
	}

	settle := 2*(retryUp+retryDown) + 6
	sim.trace = append(sim.trace, fmt.Sprintf("-- %d runs with no further changes", settle))
	for r := 0; r < settle; r++ {
		ok, err := sim.step()
		sim.checkRun(ht, ok, err)
	}
	before, _ := sim.layout()
	ok, err := sim.step()
	sim.checkRun(ht, ok, err)
	after, _ := sim.layout()
	if !reflect.DeepEqual(before, after) {
		ht.Fatalf("mysql_servers still changing after %d quiet runs:\n before: %v\n  after: %v\n%s", settle, before, after, sim.describe())
	}

	writers, _ := sim.online(100)
	readers, _ := sim.online(101)
	maxW, usable := -1, 0
	for i, s := range sim.nodes {
		if s.writerOK() {
			usable++
			if s.weight > maxW {
				maxW = s.weight
			}
		}
		if s.readerOK() && !readers[i] {
			ht.Fatalf("node %d can serve reads but is not an ONLINE reader\n%s", i, sim.describe())
		}
		if !s.readerOK() && readers[i] {
			ht.Fatalf("node %d cannot serve reads but is an ONLINE reader\n%s", i, sim.describe())
		}
		if !s.writerOK() && writers[i] {
			ht.Fatalf("node %d cannot take writes but is the ONLINE writer\n%s", i, sim.describe())
		}
	}
	if want := min(maxWriters, usable); len(writers) != want {
		ht.Fatalf("%d ONLINE writers, want %d (maxNumWriters=%d, %d nodes can take writes)\n%s", len(writers), want, maxWriters, usable, sim.describe())
	}
	if maxW >= 0 && maxWriters == 1 && sim.cfg.Pxcluster.FailBack {
		for i := range writers {
			if sim.nodes[i].weight != maxW {
				ht.Fatalf("failBack on: writer is node %d (weight %d), the heaviest usable node has weight %d\n%s", i, sim.nodes[i].weight, maxW, sim.describe())
			}
		}
	}
}

// runsUntil runs the scheduler until cond holds and returns how many runs that took (0 if it never did).
func (s *simCluster) runsUntil(ht *hegel.T, limit int, cond func() bool) int {
	for r := 1; r <= limit; r++ {
		ok, err := s.step()
		s.checkRun(ht, ok, err)
		if cond() {
			return r
		}
	}
	return 0
}

// A fault takes a node out after retryDown runs (at least one; maintenance at once), and a repair
// brings it back after retryUp runs (at least one). The second fault on the same node takes as
// long as the first: counters from the first one must not carry over.
func TestProperty_MultiRun_RetryDelays(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		retryUp := hegel.Draw(ht, hegel.Integers(0, 4))
		retryDown := hegel.Draw(ht, hegel.Integers(0, 4))
		// node 0 is the writer; the fault hits the writer or a reader
		nodes := []pxcState{healthyPxc(300), healthyPxc(200), healthyPxc(100)}
		sim := newSimCluster(t, nodes, retryUp, retryDown, false)
		for r := 0; r < 2; r++ {
			ok, err := sim.step()
			sim.checkRun(ht, ok, err)
		}

		target := hegel.Draw(ht, hegel.SampledFrom([]int{0, 1}))
		fault := hegel.Draw(ht, hegel.SampledFrom([]string{"nonPrimary", "reject", "maint"}))
		wantDown := max(retryDown, 1)
		if fault == "maint" {
			wantDown = 1
		}
		wantUp := max(retryUp, 1)
		limit := 12

		for cycle := 1; cycle <= 2; cycle++ {
			// watch the writer hostgroup if the node is the writer now, else the reader hostgroup
			hg := 101
			if writers, _ := sim.online(100); writers[target] {
				hg = 100
			}
			switch fault {
			case "nonPrimary":
				sim.nodes[target].primary = false
			case "reject":
				sim.nodes[target].reject = true
			case "maint":
				sim.nodes[target].maint = true
			}
			sim.trace = append(sim.trace, fmt.Sprintf("cycle %d: node %d %s", cycle, target, fault))
			got := sim.runsUntil(ht, limit, func() bool { on, _ := sim.online(hg); return !on[target] })
			if got != wantDown {
				ht.Fatalf("cycle %d: node %d (%s) left HG %d after %d runs, want %d (retryDown=%d; 0 = not within %d)\n%s",
					cycle, target, fault, hg, got, wantDown, retryDown, limit, sim.describe())
			}

			sim.nodes[target] = healthyPxc(sim.nodes[target].weight)
			sim.trace = append(sim.trace, fmt.Sprintf("cycle %d: node %d healed", cycle, target))
			got = sim.runsUntil(ht, limit, func() bool { on, _ := sim.online(101); return on[target] })
			if got != wantUp {
				ht.Fatalf("cycle %d: node %d came back as reader after %d runs, want %d (retryUp=%d; 0 = not within %d)\n%s",
					cycle, target, got, wantUp, retryUp, limit, sim.describe())
			}
			// let the scheduler finish (writer back, counters reset) before the next fault
			for r := 0; r < 2*(retryUp+retryDown)+4; r++ {
				ok, err := sim.step()
				sim.checkRun(ht, ok, err)
			}
		}
	}, hegel.WithTestCases(simCases(200)))
}

// simCases returns the number of cases for a simulation test. PXC_SIM_CASES lowers it for long
// runs that repeat the suite many times, such as mutation testing.
func simCases(def int) int {
	if n, err := strconv.Atoi(os.Getenv("PXC_SIM_CASES")); err == nil && n > 0 {
		return n
	}
	return def
}
