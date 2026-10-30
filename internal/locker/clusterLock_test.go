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

package locker

import (
	"database/sql"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"pxc_scheduler_handler/internal/proxysql"
	"pxc_scheduler_handler/internal/testutil"

	"hegel.dev/go/hegel"
)

// ****************  cluster lock helpers **********************************

// proxysql_servers as defined by ProxySQL; in a ProxySQL cluster it is synced to every node,
// so the tests use one shared table for all schedulers.
const proxysqlServersDDL = `CREATE TABLE proxysql_servers (
	hostname VARCHAR NOT NULL,
	port INT NOT NULL DEFAULT 6032,
	weight INT CHECK (weight >= 0) NOT NULL DEFAULT 0,
	comment VARCHAR NOT NULL DEFAULT '',
	PRIMARY KEY (hostname, port))`

const testClusterLockId = "1_HG_100_W_HG_101_R"

func newLockDB(t *testing.T, comments []string) *sql.DB {
	db, _ := testutil.NewAdminDB(t)
	if _, err := db.Exec(proxysqlServersDDL); err != nil {
		t.Fatal(err)
	}
	for i, c := range comments {
		if _, err := db.Exec("INSERT INTO proxysql_servers (hostname,port,weight,comment) VALUES (?,?,?,?)", proxyIp(i), 6032, 1, c); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func proxyIp(i int) string  { return "10.0.1." + strconv.Itoa(i+1) }
func proxyDns(i int) string { return net.JoinHostPort(proxyIp(i), "6032") }

// readProxyNodes builds the node map the way GetProxySQLnodes does, with every node reachable.
func readProxyNodes(db *sql.DB) (map[string]proxysql.ProxySQLNodeImpl, error) {
	rows, err := db.Query("SELECT weight,hostname,port,comment FROM proxysql_servers ORDER BY weight DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	nodes := map[string]proxysql.ProxySQLNodeImpl{}
	for rows.Next() {
		var n proxysql.ProxySQLNodeImpl
		if err := rows.Scan(&n.Weight, &n.Ip, &n.Port, &n.Comment); err != nil {
			return nil, err
		}
		n.Dns = net.JoinHostPort(n.Ip, strconv.Itoa(n.Port))
		nodes[n.Dns] = n
	}
	return nodes, rows.Err()
}

func newTestLocker(db *sql.DB, i int, clusterLockId string, timeout, refresh int64) *LockerImpl {
	return &LockerImpl{
		ClusterLockId:      clusterLockId,
		LockClusterTimeout: timeout,
		LockRefreshTime:    refresh,
		MyServer:           &proxysql.ProxySQLNodeImpl{Dns: proxyDns(i), Ip: proxyIp(i), Port: 6032, Connection: db, IsInitialized: true},
	}
}

// runClusterLock does what CheckClusterLock does after GetProxySQLnodes: it reports whether this
// scheduler may act, and whether it wrote its lock. A panic is returned as an error.
func runClusterLock(db *sql.DB, locker *LockerImpl) (acted bool, wrote bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	nodes, err := readProxyNodes(db)
	if err != nil {
		return false, false, err
	}
	toWrite, ok := locker.findLock(nodes)
	if !ok {
		return false, false, nil
	}
	if !locker.PushSchedulerLock(toWrite) {
		return false, false, nil
	}
	return true, len(toWrite) > 0, nil
}

func lockComment(clusterLockId string, at time.Time) string {
	return "#LOCK_" + clusterLockId + "_" + strconv.FormatInt(at.UnixNano(), 10) + "_LOCK#"
}

var lockTimeRe = regexp.MustCompile(`(#LOCK_[^#]*?_)(\d+)(_LOCK#)`)

// ageLocks moves every lock timestamp in proxysql_servers back by the given seconds,
// which to findLock looks the same as that much time passing.
func ageLocks(db *sql.DB, seconds int64) error {
	nodes, err := readProxyNodes(db)
	if err != nil {
		return err
	}
	for _, n := range nodes {
		aged := lockTimeRe.ReplaceAllStringFunc(n.Comment, func(m string) string {
			parts := lockTimeRe.FindStringSubmatch(m)
			ts, _ := strconv.ParseInt(parts[2], 10, 64)
			return parts[1] + strconv.FormatInt(ts-seconds*1000000000, 10) + parts[3]
		})
		if _, err := db.Exec("UPDATE proxysql_servers SET comment=? WHERE hostname=? AND port=?", aged, n.Ip, n.Port); err != nil {
			return err
		}
	}
	return nil
}

// ****************  findLock / PushSchedulerLock **********************************

func TestClusterLock_Scenarios(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name      string
		comments  []string
		me        int
		wantActed bool
		wantWrote bool
	}{
		{"no locks: first scheduler takes the lock", []string{"", ""}, 0, true, true},
		{"another node holds a fresh lock", []string{"", lockComment(testClusterLockId, now.Add(-5*time.Second))}, 0, false, false},
		{"another node's lock has expired", []string{"", lockComment(testClusterLockId, now.Add(-120*time.Second))}, 0, true, true},
		{"own fresh lock, too soon to refresh", []string{lockComment(testClusterLockId, now.Add(-5*time.Second)), ""}, 0, true, false},
		{"own lock due for refresh", []string{lockComment(testClusterLockId, now.Add(-50*time.Second)), ""}, 0, true, true},
		{"lock of another cluster is ignored", []string{"", lockComment("2_HG_200_W_HG_201_R", now.Add(-5*time.Second))}, 0, true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newLockDB(t, tt.comments)
			acted, wrote, err := runClusterLock(db, newTestLocker(db, tt.me, testClusterLockId, 60, 45))
			if err != nil {
				t.Fatal(err)
			}
			if acted != tt.wantActed || wrote != tt.wantWrote {
				t.Errorf("acted=%v wrote=%v, want acted=%v wrote=%v", acted, wrote, tt.wantActed, tt.wantWrote)
			}
		})
	}
}

// One ProxySQL can run schedulers for several PXC clusters; each keeps its own lock in the same
// comment. The scheduler of the second cluster must still read its own lock.
func TestClusterLock_TwoClustersOnOneProxySQL(t *testing.T) {
	now := time.Now()
	other := "2_HG_200_W_HG_201_R"
	comment := lockComment(testClusterLockId, now.Add(-5*time.Second)) + " " + lockComment(other, now.Add(-5*time.Second))
	db := newLockDB(t, []string{"", comment})

	// node 0 asks for cluster 2's lock, held by node 1 and still fresh
	acted, _, err := runClusterLock(db, newTestLocker(db, 0, other, 60, 45))
	if err != nil {
		t.Fatalf("findLock for cluster %s: %v (comment %q)", other, err, comment)
	}
	if acted {
		t.Errorf("node 0 acted for cluster %s while node 1 holds a fresh lock", other)
	}
}

// A quote in a ProxySQL node's comment must not stop the cluster lock from being written.
func TestClusterLock_QuoteInComment(t *testing.T) {
	db := newLockDB(t, []string{"Bob's proxy", ""})
	acted, wrote, err := runClusterLock(db, newTestLocker(db, 0, testClusterLockId, 60, 45))
	if err != nil {
		t.Fatal(err)
	}
	if !acted || !wrote {
		t.Errorf("acted=%v wrote=%v, want the scheduler to take the free lock", acted, wrote)
	}
}

// ****************  boundaries and gaps found by mutation testing **********************************

// A lock exactly LockClusterTimeout seconds old is still valid; one second more and it has expired.
func TestClusterLock_ExpiryBoundary(t *testing.T) {
	for _, tt := range []struct {
		age       time.Duration
		wantActed bool
	}{{60 * time.Second, false}, {61 * time.Second, true}} {
		db := newLockDB(t, []string{"", lockComment(testClusterLockId, time.Now().Add(-tt.age))})
		acted, _, err := runClusterLock(db, newTestLocker(db, 0, testClusterLockId, 60, 45))
		if err != nil {
			t.Fatal(err)
		}
		if acted != tt.wantActed {
			t.Errorf("other node's lock %v old (timeout 60s): acted=%v, want %v", tt.age, acted, tt.wantActed)
		}
	}
}

// The holder rewrites its lock once it is LockRefreshTime seconds old, not before.
func TestClusterLock_RefreshBoundary(t *testing.T) {
	for _, tt := range []struct {
		age       time.Duration
		wantWrote bool
	}{{44 * time.Second, false}, {45 * time.Second, true}} {
		db := newLockDB(t, []string{lockComment(testClusterLockId, time.Now().Add(-tt.age)), ""})
		acted, wrote, err := runClusterLock(db, newTestLocker(db, 0, testClusterLockId, 60, 45))
		if err != nil {
			t.Fatal(err)
		}
		if !acted || wrote != tt.wantWrote {
			t.Errorf("own lock %v old (refresh 45s): acted=%v wrote=%v, want acted=true wrote=%v", tt.age, acted, wrote, tt.wantWrote)
		}
	}
}

// Taking over an expired lock removes it from the other node's comment and keeps the rest of the comment.
func TestClusterLock_TakeoverCleansComment(t *testing.T) {
	expired := lockComment(testClusterLockId, time.Now().Add(-120*time.Second))
	db := newLockDB(t, []string{"", "proxy B " + expired + " rack 2"})
	if acted, wrote, err := runClusterLock(db, newTestLocker(db, 0, testClusterLockId, 60, 45)); err != nil || !acted || !wrote {
		t.Fatalf("acted=%v wrote=%v err=%v, want the scheduler to take the expired lock", acted, wrote, err)
	}
	nodes, err := readProxyNodes(db)
	if err != nil {
		t.Fatal(err)
	}
	if got := nodes[proxyDns(1)].Comment; got != "proxy B rack 2" {
		t.Errorf("node 1 comment = %q, want %q", got, "proxy B rack 2")
	}
	if got := nodes[proxyDns(0)].Comment; !strings.Contains(got, "#LOCK_"+testClusterLockId+"_") {
		t.Errorf("node 0 comment = %q, want it to hold the new lock", got)
	}
}

// When the transaction for the lock cannot be started (ProxySQL admin connection lost),
// PushSchedulerLock reports failure instead of crashing.
func TestPushSchedulerLock_ConnectionLost(t *testing.T) {
	db := newLockDB(t, []string{"", ""})
	locker := newTestLocker(db, 0, testClusterLockId, 60, 45)
	nodes, err := readProxyNodes(db)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("PushSchedulerLock() panicked: %v", r)
		}
	}()
	if locker.PushSchedulerLock(nodes) {
		t.Error("PushSchedulerLock() = true on a closed connection, want false")
	}
}

// deadPid returns the PID of a process that has already exited.
func deadPid(t *testing.T) int {
	cmd := exec.Command("true")
	if err := cmd.Run(); err != nil {
		t.Skip("cannot spawn helper process:", err)
	}
	return cmd.Process.Pid
}

// A lock file whose owner is gone is kept until it is older than the timeout, not at exactly the timeout.
func TestEvaluateFileLockForRemoval_TimeoutBoundary(t *testing.T) {
	dead := deadPid(t)
	const lockTime = int64(1_000_000_000_000)
	for _, tt := range []struct {
		elapsed int64
		want    bool
	}{{60, false}, {61, true}} {
		fl := FileLockImp{flPid: os.Getpid(), flTimeout: 60, flAppCreationTime: lockTime + tt.elapsed*1000000000}
		if got := fl.EvaluateFileLockForRemoval(true, dead, lockTime); got != tt.want {
			t.Errorf("dead owner, %ds old, timeout 60s: removable=%v, want %v", tt.elapsed, got, tt.want)
		}
	}
}

// SetLock replaces a lock file left by a process that is gone and has timed out.
func TestSetLock_ReplacesStaleLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lock")
	now := time.Now().UnixNano()
	old := fmt.Sprintf("PID:%d\nTime:%d\n", deadPid(t), now-120*1000000000)
	if err := os.WriteFile(path, []byte(old), 0600); err != nil {
		t.Fatal(err)
	}
	// a PID with more digits than any real one, so the new content is at least as long as the old
	fl := FileLockImp{flPid: 999999999, flFullPath: path, flTimeout: 60, flAppCreationTime: now}
	if !fl.SetLock() {
		t.Fatal("SetLock() = false over a stale lock")
	}
	if exists, pid, ts := fl.CheckLockFIleExists(); !exists || pid != 999999999 || ts != now {
		t.Errorf("lock file now holds pid=%d time=%d, want %d/%d", pid, ts, 999999999, now)
	}
}

// A new lock shorter than the stale one it replaces leaves nothing of the old file behind.
func TestSetLock_ShorterLockLeavesNoTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lock")
	now := time.Now().UnixNano()
	old := fmt.Sprintf("PID:%010d\nTime:%d\nTime:1\n", deadPid(t), now-120*1000000000)
	if err := os.WriteFile(path, []byte(old), 0600); err != nil {
		t.Fatal(err)
	}
	fl := FileLockImp{flPid: 42, flFullPath: path, flTimeout: 60, flAppCreationTime: now}
	if !fl.SetLock() {
		t.Fatal("SetLock() = false over a stale lock")
	}
	content, _ := os.ReadFile(path)
	if want := fmt.Sprintf("PID:42\nTime:%d\n", now); string(content) != want {
		t.Errorf("lock file content = %q, want %q", content, want)
	}
}

// ****************  property-based tests (hegel) **********************************

// Any comment in proxysql_servers, including broken or hand-edited lock text, is read without panicking.
func TestProperty_ClusterLock_AnyComment(t *testing.T) {
	header := "#LOCK_" + testClusterLockId + "_"
	fragments := []string{header, "_LOCK#", "#LOCK_2_HG_200_W_HG_201_R_", "123", strconv.FormatInt(time.Now().UnixNano(), 10), " ", "x", "#"}
	db := newLockDB(t, []string{"", ""})
	hegel.Test(t, func(ht *hegel.T) {
		parts := hegel.Draw(ht, hegel.Lists(hegel.SampledFrom(fragments)).MaxSize(6))
		comment := strings.Join(parts, "")
		if _, err := db.Exec("UPDATE proxysql_servers SET comment=? WHERE hostname=?", comment, proxyIp(1)); err != nil {
			ht.Fatalf("update: %v", err)
		}
		if _, err := db.Exec("UPDATE proxysql_servers SET comment='' WHERE hostname=?", proxyIp(0)); err != nil {
			ht.Fatalf("update: %v", err)
		}
		if _, _, err := runClusterLock(db, newTestLocker(db, 0, testClusterLockId, 60, 45)); err != nil {
			ht.Fatalf("comment %q on the other node: %v", comment, err)
		}
	}, hegel.WithTestCases(500))
}

// Several ProxySQL nodes run the scheduler against the shared proxysql_servers table while time
// passes and schedulers stop. A scheduler may act only when no other scheduler holds a lock that
// has not expired, and a scheduler that finds no such lock always acts.
//
// Cron mode starts a new process (a new LockerImpl) for every run; daemon mode keeps one
// LockerImpl per scheduler for all runs, as the main loop does.
func TestProperty_ClusterLock_OneSchedulerActs(t *testing.T) {
	const timeout = 60
	hegel.Test(t, func(ht *hegel.T) {
		n := hegel.Draw(ht, hegel.Integers(2, 3))
		daemon := hegel.Draw(ht, hegel.Booleans())
		refresh := int64(hegel.Draw(ht, hegel.Integers(1, 44)))

		db := newLockDB(t, make([]string, n))
		lockers := make([]*LockerImpl, n)
		for i := range lockers {
			lockers[i] = newTestLocker(db, i, testClusterLockId, timeout, refresh)
		}
		lastWrite := map[int]int64{} // sim time of each scheduler's current lock in the table
		var simNow int64
		var history []string

		steps := hegel.Draw(ht, hegel.Integers(3, 25))
		for s := 0; s < steps; s++ {
			if hegel.Draw(ht, hegel.WeightedBooleans(0.3)) {
				d := int64(hegel.Draw(ht, hegel.Integers(1, 90)))
				if err := ageLocks(db, d); err != nil {
					ht.Fatalf("age: %v", err)
				}
				simNow += d
				history = append(history, fmt.Sprintf("t=%d: %ds pass", simNow, d))
				continue
			}
			i := hegel.Draw(ht, hegel.Integers(0, n-1))
			locker := lockers[i]
			if !daemon {
				locker = newTestLocker(db, i, testClusterLockId, timeout, refresh)
			}
			acted, wrote, err := runClusterLock(db, locker)
			history = append(history, fmt.Sprintf("t=%d: scheduler %d acted=%v wrote=%v", simNow, i, acted, wrote))
			if err != nil {
				ht.Fatalf("scheduler %d: %v\n%s", i, err, strings.Join(history, "\n"))
			}

			holder := -1
			for j, at := range lastWrite {
				if j != i && simNow-at <= timeout {
					holder = j
				}
			}
			if acted && holder >= 0 {
				ht.Fatalf("daemon=%v refresh=%ds: scheduler %d acted while scheduler %d holds a lock written %ds ago (timeout %ds)\n%s",
					daemon, refresh, i, holder, simNow-lastWrite[holder], timeout, strings.Join(history, "\n"))
			}
			if !acted && holder < 0 {
				ht.Fatalf("daemon=%v refresh=%ds: scheduler %d did not act although no other scheduler holds a valid lock\n%s",
					daemon, refresh, i, strings.Join(history, "\n"))
			}
			if wrote {
				// the winner rewrites every node's comment without the other locks
				lastWrite = map[int]int64{i: simNow}
			}
		}
	}, hegel.WithTestCases(300))
}
