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

package global

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	log "github.com/sirupsen/logrus"
)

// ****************  boundaries and gaps found by mutation testing **********************************

// captureLog sends log output at the given level to a buffer for the rest of the test.
func captureLog(t *testing.T, level log.Level) *bytes.Buffer {
	var buf bytes.Buffer
	origOut, origLevel := log.StandardLogger().Out, log.GetLevel()
	log.SetOutput(&buf)
	log.SetLevel(level)
	t.Cleanup(func() {
		log.SetOutput(origOut)
		log.SetLevel(origLevel)
	})
	return &buf
}

func TestSanityCheck_Boundaries(t *testing.T) {
	t.Run("one writer with WriterIsAlsoReader=0 is accepted", func(t *testing.T) {
		c := validConfig(t)
		c.Pxcluster.WriterIsAlsoReader = 0
		c.Pxcluster.MaxWriters = 1
		if !c.SanityCheck() {
			t.Error("SanityCheck() = false, want true")
		}
	})
	t.Run("ping timeout equal to CheckTimeOut is halved", func(t *testing.T) {
		c := validConfig(t)
		c.Pxcluster.CheckTimeOut = 1000
		c.Proxysql.PingTimeout = 1000
		c.Pxcluster.PingTimeout = 1000
		c.SanityCheck()
		if c.Proxysql.PingTimeout != 500 || c.Pxcluster.PingTimeout != 500 {
			t.Errorf("ping timeouts = %d/%d, want 500/500", c.Proxysql.PingTimeout, c.Pxcluster.PingTimeout)
		}
	})
	t.Run("ping timeout just below CheckTimeOut is kept", func(t *testing.T) {
		c := validConfig(t)
		c.Pxcluster.CheckTimeOut = 1000
		c.Proxysql.PingTimeout = 999
		c.Pxcluster.PingTimeout = 999
		c.SanityCheck()
		if c.Proxysql.PingTimeout != 999 || c.Pxcluster.PingTimeout != 999 {
			t.Errorf("ping timeouts = %d/%d, want 999/999", c.Proxysql.PingTimeout, c.Pxcluster.PingTimeout)
		}
	})
	t.Run("lock refresh at exactly 3/4 of the timeout is kept", func(t *testing.T) {
		c := validConfig(t)
		c.Global.LockClusterTimeout = 120
		c.Global.LockRefreshTime = 90
		c.SanityCheck()
		if c.Global.LockRefreshTime != 90 {
			t.Errorf("LockRefreshTime = %d, want 90", c.Global.LockRefreshTime)
		}
	})
	t.Run("missing SSL path is reported", func(t *testing.T) {
		buf := captureLog(t, log.WarnLevel)
		c := validConfig(t)
		c.Pxcluster.SslcertificatePath = filepath.Join(t.TempDir(), "missing")
		c.SanityCheck()
		if !strings.Contains(buf.String(), "SSL Path is not accessible") {
			t.Errorf("no warning for a missing SSL path; log:\n%s", buf.String())
		}
	})
	t.Run("no SSL path, no SSL warnings", func(t *testing.T) {
		buf := captureLog(t, log.WarnLevel)
		c := validConfig(t)
		c.Pxcluster.SslcertificatePath = ""
		c.SanityCheck()
		if strings.Contains(buf.String(), "SSL") {
			t.Errorf("SSL warning without an SSL path; log:\n%s", buf.String())
		}
	})
}

func TestOrderedPerfMap_IteratorIndex(t *testing.T) {
	m := NewOrderedMap()
	for _, k := range []string{"a", "b", "c"} {
		m.Set(k, PerfObject{Name: k})
	}
	it := m.Iterator()
	for want := 0; ; want++ {
		i, key, _ := it()
		if i == nil {
			if want != 3 {
				t.Errorf("iteration stopped after %d entries, want 3", want)
			}
			break
		}
		if *i != want {
			t.Errorf("index for %s = %d, want %d", *key, *i, want)
		}
	}
}

// ReportPerformance logs every phase at or above the current level, with its duration in ns and ms.
func TestReportPerformance_Output(t *testing.T) {
	PerformanceMapOrdered = NewOrderedMap()
	PerformanceMapOrdered.Set("first", PerfObject{Name: "first", LogLevel: log.InfoLevel, Time: [2]int64{1000, 2001000}})
	PerformanceMapOrdered.Set("second", PerfObject{Name: "second", LogLevel: log.InfoLevel, Time: [2]int64{0, 5000000}})
	PerformanceMapOrdered.Set("hidden", PerfObject{Name: "hidden", LogLevel: log.DebugLevel, Time: [2]int64{0, 1}})
	buf := captureLog(t, log.InfoLevel)

	ReportPerformance()
	out := buf.String()
	for _, want := range []string{
		"Reporting execution times",
		"Phase: first = 2,000,000 ns 2 ms",
		"Phase: second = 5,000,000 ns 5 ms",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("ReportPerformance() output missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "hidden") {
		t.Errorf("ReportPerformance() logged a debug-level phase at info level:\n%s", out)
	}
}
