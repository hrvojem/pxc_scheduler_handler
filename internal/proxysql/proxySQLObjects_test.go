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
	"reflect"
	"strconv"
	"strings"
	"testing"

	global "pxc_scheduler_handler/internal/global"

	"hegel.dev/go/hegel"
)

type serverRow struct {
	hg         int
	host       string
	port       int
	status     string
	weight     int
	maxConn    int
	maxLag     int
	maxLatency int
	comment    string
}

func (r serverRow) key() string { return net.JoinHostPort(r.host, strconv.Itoa(r.port)) }

func clearServers(db *sql.DB) error {
	_, err := db.Exec("DELETE FROM mysql_servers")
	return err
}

func insertServer(db *sql.DB, r serverRow) error {
	_, err := db.Exec("INSERT INTO mysql_servers (hostgroup_id,hostname,port,status,weight,max_connections,max_replication_lag,max_latency_ms,comment) VALUES (?,?,?,?,?,?,?,?,?)",
		r.hg, r.host, r.port, r.status, r.weight, r.maxConn, r.maxLag, r.maxLatency, r.comment)
	return err
}

func readServers(db *sql.DB) ([]serverRow, error) {
	rows, err := db.Query("SELECT hostgroup_id,hostname,port,status,weight,max_connections,max_replication_lag,max_latency_ms,comment FROM mysql_servers ORDER BY hostgroup_id,hostname,port")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []serverRow
	for rows.Next() {
		var r serverRow
		if err := rows.Scan(&r.hg, &r.host, &r.port, &r.status, &r.weight, &r.maxConn, &r.maxLag, &r.maxLatency, &r.comment); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func readComment(db *sql.DB, hg int, host string, port int) (string, error) {
	var c string
	err := db.QueryRow("SELECT comment FROM mysql_servers WHERE hostgroup_id=? AND hostname=? AND port=?", hg, host, port).Scan(&c)
	return c, err
}

func newProxyNode(db *sql.DB, cluster *DataClusterImpl) *ProxySQLNodeImpl {
	cfg := &global.Configuration{}
	cfg.Pxcluster.MaintenanceHgRange = cluster.MaintenanceHgRange
	return &ProxySQLNodeImpl{Connection: db, MySQLCluster: cluster, Config: cfg, ActionNodeList: map[string]DataNodeImpl{}}
}

// ****************  SQL builders **********************************

func TestSQLBuilders(t *testing.T) {
	db, _ := newAdminDB(t)
	cluster := newTestCluster()
	proxy := newProxyNode(db, cluster)
	node := healthyNode("10.0.0.1:3306", 101, 5)
	const host, port = "10.0.0.1", 3306

	tests := []struct {
		name  string
		seed  []serverRow
		sql   string
		check func(rows []serverRow) string
	}{
		{"move down to offline soft", []serverRow{{hg: 100, host: host, port: port, status: "ONLINE"}},
			proxy.MoveNodeDownToOfflineSoft(node, 100, host, port),
			func(rows []serverRow) string {
				if len(rows) != 1 || rows[0].status != "OFFLINE_SOFT" {
					return fmt.Sprintf("want one OFFLINE_SOFT row, got %+v", rows)
				}
				return ""
			}},
		{"move up from offline soft", []serverRow{{hg: 100, host: host, port: port, status: "OFFLINE_SOFT"}},
			proxy.MoveNodeUpFromOfflineSoft(node, 100, host, port),
			func(rows []serverRow) string {
				if len(rows) != 1 || rows[0].status != "ONLINE" {
					return fmt.Sprintf("want one ONLINE row, got %+v", rows)
				}
				return ""
			}},
		{"move down to maintenance HG", []serverRow{{hg: 101, host: host, port: port, status: "ONLINE"}},
			proxy.MoveNodeDownToHGCange(node, 101, host, port),
			func(rows []serverRow) string {
				if len(rows) != 1 || rows[0].hg != 9101 {
					return fmt.Sprintf("want the row in HG 9101, got %+v", rows)
				}
				return ""
			}},
		{"move up from maintenance HG", []serverRow{{hg: 9101, host: host, port: port, status: "ONLINE"}},
			proxy.MoveNodeUpFromHGCange(node, 9101, host, port),
			func(rows []serverRow) string {
				if len(rows) != 1 || rows[0].hg != 101 {
					return fmt.Sprintf("want the row in HG 101, got %+v", rows)
				}
				return ""
			}},
		{"delete only touches the given HG", []serverRow{{hg: 100, host: host, port: port, status: "ONLINE"}, {hg: 101, host: host, port: port, status: "ONLINE"}},
			proxy.DeleteDataNode(node, 100, host, port),
			func(rows []serverRow) string {
				if len(rows) != 1 || rows[0].hg != 101 {
					return fmt.Sprintf("want only the HG 101 row left, got %+v", rows)
				}
				return ""
			}},
		{"insert write", nil, proxy.InsertWrite(node, 100, host, port),
			func(rows []serverRow) string {
				if len(rows) != 1 || rows[0].hg != 100 || rows[0].weight != 5 || rows[0].comment != node.Comment {
					return fmt.Sprintf("want one HG 100 row with weight 5, got %+v", rows)
				}
				return ""
			}},
		{"insert read", nil, proxy.InsertRead(node, 101, host, port),
			func(rows []serverRow) string {
				if len(rows) != 1 || rows[0].hg != 101 {
					return fmt.Sprintf("want one HG 101 row, got %+v", rows)
				}
				return ""
			}},
		{"insert new writer goes to maintenance HG", nil, proxy.InsertWrite(DataNodeImpl{NodeIsNew: true, ProxyStatus: "ONLINE", Weight: 1}, 100, host, port),
			func(rows []serverRow) string {
				if len(rows) != 1 || rows[0].hg != 9100 {
					return fmt.Sprintf("want one HG 9100 row, got %+v", rows)
				}
				return ""
			}},
		{"insert new reader goes to maintenance HG", nil, proxy.InsertRead(DataNodeImpl{NodeIsNew: true, ProxyStatus: "ONLINE", Weight: 1}, 101, host, port),
			func(rows []serverRow) string {
				if len(rows) != 1 || rows[0].hg != 9101 {
					return fmt.Sprintf("want one HG 9101 row, got %+v", rows)
				}
				return ""
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := clearServers(db); err != nil {
				t.Fatal(err)
			}
			for _, r := range tt.seed {
				if err := insertServer(db, r); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := db.Exec(tt.sql); err != nil {
				t.Fatalf("SQL failed: %v\n%s", err, tt.sql)
			}
			rows, err := readServers(db)
			if err != nil {
				t.Fatal(err)
			}
			if msg := tt.check(rows); msg != "" {
				t.Errorf("%s\n%s", msg, tt.sql)
			}
		})
	}
}

// RESET_DEFAULTS restores the node's settings from the config hostgroup, max_latency_ms included.
func TestResetNodeDefaults(t *testing.T) {
	db, _ := newAdminDB(t)
	proxy := newProxyNode(db, newTestCluster())
	node := healthyNode("10.0.0.1:3306", 100, 7)
	node.MaxConnection = 250
	node.MaxReplicationLag = 30
	node.MaxLatency = 500

	if err := insertServer(db, serverRow{hg: 100, host: "10.0.0.1", port: 3306, status: "OFFLINE_SOFT", weight: 1, maxConn: 1, maxLag: 1, maxLatency: 1}); err != nil {
		t.Fatal(err)
	}
	stmt := proxy.ResetNodeDefaults(node, 100, "10.0.0.1", 3306)
	if _, err := db.Exec(stmt); err != nil {
		t.Fatalf("SQL failed: %v\n%s", err, stmt)
	}
	rows, err := readServers(db)
	if err != nil {
		t.Fatal(err)
	}
	got := rows[0]
	if got.status != "ONLINE" || got.weight != 7 || got.maxConn != 250 || got.maxLag != 30 {
		t.Errorf("ResetNodeDefaults() row = %+v", got)
	}
	if got.maxLatency != 500 {
		t.Errorf("max_latency_ms = %d, want %d (MaxLatency)\n%s", got.maxLatency, node.MaxLatency, stmt)
	}
}

// One node whose comment contains a quote must not stop the changes for every other node.
func TestProcessChanges_QuoteInCommentDoesNotBlockOtherChanges(t *testing.T) {
	db, _ := newAdminDB(t)
	cluster := newTestCluster()
	proxy := newProxyNode(db, cluster)

	writer := healthyNode("10.0.0.1:3306", 100, 1)
	reader := healthyNode("10.0.0.2:3306", 101, 1)
	reader.Comment = "Bob's node"
	for _, r := range []serverRow{
		{hg: 100, host: "10.0.0.1", port: 3306, status: "ONLINE", weight: 1, maxConn: 1000},
		{hg: 101, host: "10.0.0.2", port: 3306, status: "ONLINE", weight: 1, maxConn: 1000, comment: reader.Comment},
	} {
		if err := insertServer(db, r); err != nil {
			t.Fatal(err)
		}
	}

	writer.ActionType = writer.MOVE_DOWN_OFFLINE()
	reader.ActionType = reader.INSERT_WRITE()
	proxy.ActionNodeList["100_10.0.0.1:3306"] = writer
	proxy.ActionNodeList["4020_100_10.0.0.2:3306"] = reader

	if !proxy.ProcessChanges() {
		t.Error("ProcessChanges() = false")
	}
	rows, err := readServers(db)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if r.hg == 100 && r.host == "10.0.0.1" && r.status != "OFFLINE_SOFT" {
			t.Errorf("writer 10.0.0.1 status = %s, want OFFLINE_SOFT; rows: %+v", r.status, rows)
		}
	}
}

// ****************  property-based tests (hegel) **********************************

// Whatever comment a node carries, inserting it and saving its retries succeeds and keeps the comment.
func TestProperty_SQLBuilders_StoreAnyComment(t *testing.T) {
	db, _ := newAdminDB(t)
	cluster := newTestCluster()
	proxy := newProxyNode(db, cluster)
	hegel.Test(t, func(ht *hegel.T) {
		if err := clearServers(db); err != nil {
			ht.Fatalf("clear: %v", err)
		}
		node := healthyNode("10.0.0.1:3306", 100, 1)
		node.Comment = hegel.Draw(ht, hegel.Text().ExcludeCharacters("\x00").MaxSize(40))

		stmt := proxy.InsertWrite(node, 100, "10.0.0.1", 3306)
		if _, err := db.Exec(stmt); err != nil {
			ht.Fatalf("InsertWrite SQL failed for comment %q: %v\n%s", node.Comment, err, stmt)
		}
		if got, err := readComment(db, 100, "10.0.0.1", 3306); err != nil || got != node.Comment {
			ht.Fatalf("stored comment = %q (%v), want %q", got, err, node.Comment)
		}

		stmt = proxy.SaveRetry(node, 100, "10.0.0.1", 3306)
		if _, err := db.Exec(stmt); err != nil {
			ht.Fatalf("SaveRetry SQL failed for comment %q: %v\n%s", node.Comment, err, stmt)
		}
		got, err := readComment(db, 100, "10.0.0.1", 3306)
		if err != nil || !strings.HasPrefix(got, node.Comment) {
			ht.Fatalf("comment after SaveRetry = %q (%v), want it to start with %q", got, err, node.Comment)
		}
	}, hegel.WithTestCases(500))
}

// Saving and reading retries over several scheduler runs reads back the saved counters and leaves
// the rest of the comment as it was after the first save.
func TestProperty_SaveRetry_CommentDoesNotGrow(t *testing.T) {
	db, _ := newAdminDB(t)
	cluster := newTestCluster()
	proxy := newProxyNode(db, cluster)
	const host, port = "10.0.0.1", 3306
	hegel.Test(t, func(ht *hegel.T) {
		if err := clearServers(db); err != nil {
			ht.Fatalf("clear: %v", err)
		}
		// quotes, '=' and ';' are covered by other tests; single-digit counters avoid the \d regexp issue
		base := hegel.Draw(ht, hegel.Text().ExcludeCharacters("\x00'=;").MaxSize(20))
		if err := insertServer(db, serverRow{hg: 101, host: host, port: port, status: "ONLINE", weight: 1, maxConn: 1000, comment: base}); err != nil {
			ht.Fatalf("seed: %v", err)
		}

		rounds := hegel.Draw(ht, hegel.Integers(2, 6))
		var firstRest string
		lastUp, lastDown := 0, 0
		for r := 0; r < rounds; r++ {
			stored, err := readComment(db, 101, host, port)
			if err != nil {
				ht.Fatalf("read: %v", err)
			}
			node := healthyNode(net.JoinHostPort(host, strconv.Itoa(port)), 101, 1)
			node.Comment = stored
			getRetry(&node, cluster.HgWriterId, cluster.HgReaderId)
			if node.RetryUp != lastUp || node.RetryDown != lastDown {
				ht.Fatalf("run %d: read back retries %d/%d, saved %d/%d (comment %q)", r, node.RetryUp, node.RetryDown, lastUp, lastDown, stored)
			}
			if r == 1 {
				firstRest = node.Comment
			} else if r > 1 && node.Comment != firstRest {
				ht.Fatalf("run %d: comment without retry tokens = %q, after the first save it was %q", r, node.Comment, firstRest)
			}

			lastUp = hegel.Draw(ht, hegel.Integers(0, 9))
			lastDown = hegel.Draw(ht, hegel.Integers(0, 9))
			node.RetryUp, node.RetryDown = lastUp, lastDown
			if _, err := db.Exec(proxy.SaveRetry(node, 101, host, port)); err != nil {
				ht.Fatalf("SaveRetry: %v", err)
			}
		}
	}, hegel.WithTestCases(300))
}

// An action that depends on retries is deferred (only SaveRetry runs) exactly while the node's
// counter is below the configured threshold: retryUp for actions bringing a node up, retryDown
// for actions taking it down. MOVE_SWAP_READER_TO_WRITER (5001) is left out: nothing creates it.
func TestProperty_ProcessChanges_RetryGating(t *testing.T) {
	db, rec := newAdminDB(t)
	upActions := map[int]bool{1000: true, 1010: true, 3030: true}
	hegel.Test(t, func(ht *hegel.T) {
		code := hegel.Draw(ht, hegel.SampledFrom([]int{1000, 1010, 3030, 3001, 3010, 3020, 5101}))
		cluster := newTestCluster()
		cluster.RetryUp = hegel.Draw(ht, hegel.Integers(0, 3))
		cluster.RetryDown = hegel.Draw(ht, hegel.Integers(0, 3))
		proxy := newProxyNode(db, cluster)

		hg := 101
		if code == 1010 {
			hg = 9101
		}
		node := healthyNode("10.0.0.1:3306", hg, 1)
		node.ActionType = code
		node.RetryUp = hegel.Draw(ht, hegel.Integers(0, 3))
		node.RetryDown = hegel.Draw(ht, hegel.Integers(0, 3))
		proxy.ActionNodeList[strconv.Itoa(hg)+"_"+node.Dns] = node

		rec.Reset()
		proxy.ProcessChanges()
		stmts := rec.Statements()

		deferred := len(stmts) > 0
		for _, s := range stmts {
			if !strings.Contains(s, "SET comment=") {
				deferred = false
			}
		}
		want := node.RetryDown < cluster.RetryDown
		if upActions[code] {
			want = node.RetryUp < cluster.RetryUp
		}
		if deferred != want {
			ht.Fatalf("action %d with node retries up=%d down=%d, config up=%d down=%d: deferred=%v, want %v\n%s",
				code, node.RetryUp, node.RetryDown, cluster.RetryUp, cluster.RetryDown, deferred, want, strings.Join(stmts, "\n"))
		}
	}, hegel.WithTestCases(500))
}

// seedServers fills mysql_servers with the rows ProxySQL would hold for a cluster spec:
// writer and reader rows with the node's ProxySQL status, and ONLINE rows in the config hostgroups.
func seedServers(db *sql.DB, cluster *DataClusterImpl, spec clusterSpec) error {
	if err := clearServers(db); err != nil {
		return err
	}
	for i, s := range spec.nodes {
		host, portS, _ := net.SplitHostPort(specDns(i))
		port, _ := strconv.Atoi(portS)
		row := serverRow{host: host, port: port, weight: s.weight, maxConn: 300, maxLatency: 100, comment: "comment"}
		var rows []serverRow
		if s.writer {
			r := row
			r.hg, r.status = cluster.HgWriterId, s.proxyStatus
			rows = append(rows, r)
		}
		if s.reader {
			r := row
			r.hg, r.status = cluster.HgReaderId, s.proxyStatus
			rows = append(rows, r)
		}
		for _, hg := range []int{cluster.BakcupHgWriterId, cluster.BackupHgReaderId} {
			r := row
			r.hg, r.status = hg, "ONLINE"
			rows = append(rows, r)
		}
		for _, r := range rows {
			if err := insertServer(db, r); err != nil {
				return err
			}
		}
	}
	return nil
}

// applyRandomCluster draws a cluster, seeds mysql_servers, runs GetActionList and ProcessChanges,
// and returns the rows before and after.
func applyRandomCluster(ht *hegel.T, db *sql.DB) (spec clusterSpec, ok bool, before, after []serverRow) {
	spec = drawClusterSpec(ht, false)
	cluster := buildCluster(spec)
	if err := seedServers(db, cluster, spec); err != nil {
		ht.Fatalf("seed: %v", err)
	}
	var err error
	if before, err = readServers(db); err != nil {
		ht.Fatalf("read: %v", err)
	}
	proxy := newProxyNode(db, cluster)
	proxy.ActionNodeList = cluster.GetActionList()
	ok = proxy.ProcessChanges()
	if after, err = readServers(db); err != nil {
		ht.Fatalf("read: %v", err)
	}
	return spec, ok, before, after
}

// The SQL generated for any cluster state applies cleanly. A failed statement rolls back the whole
// transaction, so every other change in that run is lost too.
func TestProperty_ProcessChanges_RandomClusterApplies(t *testing.T) {
	db, rec := newAdminDB(t)
	hegel.Test(t, func(ht *hegel.T) {
		rec.Reset()
		spec, ok, before, _ := applyRandomCluster(ht, db)
		if !ok {
			ht.Fatalf("ProcessChanges() failed\n rows before: %+v\n statements:\n%s\n%s", before, strings.Join(rec.Statements(), "\n"), spec)
		}
	}, hegel.WithTestCases(300))
}

// Rows in the config hostgroups (8100/8101) are the reference for each node's settings;
// the scheduler never changes or removes them.
func TestProperty_ProcessChanges_ConfigHostgroupsUntouched(t *testing.T) {
	db, rec := newAdminDB(t)
	hegel.Test(t, func(ht *hegel.T) {
		rec.Reset()
		spec, ok, before, after := applyRandomCluster(ht, db)
		if !ok {
			return // covered by TestProperty_ProcessChanges_RandomClusterApplies
		}
		pick := func(rows []serverRow) []serverRow {
			var out []serverRow
			for _, r := range rows {
				if r.hg >= 8000 && r.hg < 9000 {
					out = append(out, r)
				}
			}
			return out
		}
		if !reflect.DeepEqual(pick(before), pick(after)) {
			ht.Fatalf("config hostgroup rows changed:\n before: %+v\n  after: %+v\n statements:\n%s\n%s",
				pick(before), pick(after), strings.Join(rec.Statements(), "\n"), spec)
		}
	}, hegel.WithTestCases(300))
}

// After a run, a node is never both in an active hostgroup (100/101) and in its maintenance
// hostgroup (9100/9101).
func TestProperty_ProcessChanges_NotActiveAndInMaintenance(t *testing.T) {
	db, rec := newAdminDB(t)
	hegel.Test(t, func(ht *hegel.T) {
		rec.Reset()
		spec, ok, _, after := applyRandomCluster(ht, db)
		if !ok {
			return // covered by TestProperty_ProcessChanges_RandomClusterApplies
		}
		in := map[string]bool{}
		for _, r := range after {
			in[strconv.Itoa(r.hg)+"_"+r.key()] = true
		}
		for _, r := range after {
			if (r.hg == 100 || r.hg == 101) && in[strconv.Itoa(r.hg+9000)+"_"+r.key()] {
				ht.Fatalf("%s is in HG %d and in HG %d\n rows: %+v\n statements:\n%s\n%s",
					r.key(), r.hg, r.hg+9000, after, strings.Join(rec.Statements(), "\n"), spec)
			}
		}
	}, hegel.WithTestCases(300))
}
