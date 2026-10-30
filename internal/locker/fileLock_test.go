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
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	global "pxc_scheduler_handler/internal/global"

	log "github.com/sirupsen/logrus"
	"hegel.dev/go/hegel"
)

func TestMain(m *testing.M) {
	log.SetLevel(log.PanicLevel)
	os.Exit(m.Run())
}

// ****************  file locks **********************************

func TestLockerImpl_InitAndLockFile(t *testing.T) {
	dir := t.TempDir()
	var config global.Configuration
	config.Proxysql.Host = "127.0.0.1"
	config.Proxysql.Port = 6032
	config.Proxysql.LockFilePath = dir
	config.Pxcluster.ClusterId = 10
	config.Pxcluster.HgW = 100
	config.Pxcluster.HgR = 101
	config.Global.LockFileTimeout = 60

	var locker LockerImpl
	if !locker.Init(&config) {
		t.Fatal("Init() = false")
	}
	if locker.ClusterLockId != "10_HG_100_W_HG_101_R" || locker.FileLock != locker.ClusterLockId {
		t.Errorf("ClusterLockId = %q", locker.ClusterLockId)
	}
	fl := locker.GetFileLock()
	if fl.flPid != os.Getpid() || fl.flFullPath != filepath.Join(dir, locker.ClusterLockId) || fl.flTimeout != 60 {
		t.Errorf("file lock = %+v", fl)
	}

	locker.StoreFileLock(nil) // must be ignored
	if !reflect.DeepEqual(locker.GetFileLock(), fl) {
		t.Error("StoreFileLock(nil) changed the lock")
	}

	if !locker.SetLockFile() {
		t.Fatal("SetLockFile() = false")
	}
	if locker.SetLockFile() {
		t.Error("SetLockFile() = true while lock exists and not looped")
	}
	locker.isLooped = true
	if !locker.SetLockFile() {
		t.Error("SetLockFile() = false in looped mode")
	}
	if !locker.RemoveLockFile() {
		t.Error("RemoveLockFile() = false")
	}
	if global.CheckIfPathExists(fl.flFullPath) {
		t.Error("lock file still exists after RemoveLockFile()")
	}

	locker.FileLock = ""
	if locker.SetLockFile() {
		t.Error("SetLockFile() = true with empty file name")
	}
}

func TestFileLockImp_SetLockAndRead(t *testing.T) {
	var fl FileLockImp
	if fl.init(0) {
		t.Error("init(0) = true")
	}
	if !fl.init(os.Getpid()) {
		t.Fatal("init(pid) = false")
	}
	fl.flFullPath = filepath.Join(t.TempDir(), "lock")
	fl.flAppCreationTime = 1619616760432224000

	if exists, _, _ := fl.CheckLockFIleExists(); exists {
		t.Fatal("CheckLockFIleExists() = true before SetLock()")
	}
	if !fl.SetLock() {
		t.Fatal("SetLock() = false")
	}
	exists, pid, ts := fl.CheckLockFIleExists()
	if !exists || pid != os.Getpid() || ts != fl.flAppCreationTime {
		t.Errorf("CheckLockFIleExists() = %v, %d, %d", exists, pid, ts)
	}

	// Same PID owns the lock: it must not be removed, so SetLock refuses.
	if fl.SetLock() {
		t.Error("SetLock() = true while own lock exists")
	}

	bad := FileLockImp{flPid: 1, flFullPath: filepath.Join(t.TempDir(), "missing-dir", "lock")}
	if bad.SetLock() {
		t.Error("SetLock() = true for unwritable path")
	}
}

// ****************  property-based tests (hegel) **********************************

// Whatever PID and timestamp a lock file is written with, reading it gives them back.
func TestProperty_FileLock_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	hegel.Test(t, func(ht *hegel.T) {
		pid := hegel.Draw(ht, hegel.Integers(1, math.MaxInt32))
		created := hegel.Draw(ht, hegel.Integers[int64](0, math.MaxInt64))
		path := filepath.Join(dir, "lock")
		defer os.Remove(path)

		fl := FileLockImp{flPid: pid, flFullPath: path, flAppCreationTime: created}
		if !fl.SetLock() {
			ht.Fatalf("SetLock() = false")
		}
		exists, gotPid, gotTime := fl.CheckLockFIleExists()
		if !exists || gotPid != pid || gotTime != created {
			ht.Fatalf("CheckLockFIleExists() = %v, %d, %d, want true, %d, %d", exists, gotPid, gotTime, pid, created)
		}
	}, hegel.WithTestCases(500))
}

// Reading a lock file with arbitrary (corrupt, truncated, hand-edited) content must not panic.
func TestProperty_FileLock_ReadAnyContent(t *testing.T) {
	dir := t.TempDir()
	hegel.Test(t, func(ht *hegel.T) {
		lines := hegel.Draw(ht, hegel.Lists(hegel.Text().ExcludeCharacters("\n\r").MaxSize(20)).MaxSize(5))
		path := filepath.Join(dir, "lock")
		defer os.Remove(path)
		if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0600); err != nil {
			ht.Fatalf("write: %v", err)
		}

		fl := FileLockImp{flPid: 1, flFullPath: path}
		defer func() {
			if r := recover(); r != nil {
				ht.Fatalf("CheckLockFIleExists() panicked on %q: %v", lines, r)
			}
		}()
		fl.CheckLockFIleExists()
	}, hegel.WithTestCases(1000))
}

// A lock owned by a different, still running process that has not exceeded the
// timeout must never be considered removable.
func TestProperty_FileLock_LiveOwnerWithinTimeoutNotRemovable(t *testing.T) {
	owner := os.Getpid() // a process we know is alive
	hegel.Test(t, func(ht *hegel.T) {
		myPid := hegel.Draw(ht, hegel.Integers(1, math.MaxInt32))
		ht.Assume(myPid != owner)
		timeout := hegel.Draw(ht, hegel.Integers[int64](1, 86400))
		elapsed := hegel.Draw(ht, hegel.Integers[int64](0, timeout))
		lockTime := hegel.Draw(ht, hegel.Integers[int64](0, 1<<60))

		fl := FileLockImp{flPid: myPid, flTimeout: timeout, flAppCreationTime: lockTime + elapsed*1000000000}
		if fl.EvaluateFileLockForRemoval(true, owner, lockTime) {
			ht.Fatalf("lock of live pid %d after %ds (timeout %ds) judged removable", owner, elapsed, timeout)
		}
	}, hegel.WithTestCases(500))
}

// Disabled evaluation and our own PID never allow removal, whatever the times.
func TestProperty_FileLock_DisabledOrOwnNotRemovable(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		pid := hegel.Draw(ht, hegel.Integers(1, math.MaxInt32))
		other := hegel.Draw(ht, hegel.Integers(1, math.MaxInt32))
		fl := FileLockImp{
			flPid:             pid,
			flTimeout:         hegel.Draw(ht, hegel.Integers[int64](0, 86400)),
			flAppCreationTime: hegel.Draw(ht, hegel.Integers[int64](0, math.MaxInt64)),
		}
		lockTime := hegel.Draw(ht, hegel.Integers[int64](0, math.MaxInt64))
		if fl.EvaluateFileLockForRemoval(false, other, lockTime) {
			ht.Fatal("removable with evaluate=false")
		}
		if fl.EvaluateFileLockForRemoval(true, pid, lockTime) {
			ht.Fatal("own lock judged removable")
		}
	}, hegel.WithTestCases(1000))
}

// For a lock whose owner process is gone, waiting longer never turns a
// removable lock back into a non-removable one.
func TestProperty_FileLock_DeadOwnerMonotonic(t *testing.T) {
	cmd := exec.Command("true")
	if err := cmd.Run(); err != nil {
		t.Skip("cannot spawn helper process:", err)
	}
	dead := cmd.Process.Pid
	hegel.Test(t, func(ht *hegel.T) {
		timeout := hegel.Draw(ht, hegel.Integers[int64](0, 86400))
		e1 := hegel.Draw(ht, hegel.Integers[int64](0, 200000))
		e2 := hegel.Draw(ht, hegel.Integers[int64](e1, 200000))
		lockTime := hegel.Draw(ht, hegel.Integers[int64](0, 1<<60))

		at := func(elapsed int64) bool {
			fl := FileLockImp{flPid: os.Getpid(), flTimeout: timeout, flAppCreationTime: lockTime + elapsed*1000000000}
			return fl.EvaluateFileLockForRemoval(true, dead, lockTime)
		}
		if at(e1) && !at(e2) {
			ht.Fatalf("removable after %ds but not after %ds (timeout %ds)", e1, e2, timeout)
		}
	}, hegel.WithTestCases(500))
}
