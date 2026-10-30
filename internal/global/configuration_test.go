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
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"hegel.dev/go/hegel"
)

// validConfig returns a configuration that passes SanityCheck without adjustments.
func validConfig(t *testing.T) Configuration {
	var c Configuration
	c.fillDefaults()
	c.Pxcluster.SinglePrimary = true
	c.Pxcluster.MaxNumWriters = 1
	c.Pxcluster.WriterIsAlsoReader = 1
	c.Pxcluster.HgW = 100
	c.Pxcluster.HgR = 101
	c.Pxcluster.CheckTimeOut = 4000
	c.Proxysql.PingTimeout = 1000
	c.Pxcluster.PingTimeout = 1000
	c.Proxysql.LockFilePath = t.TempDir()
	c.Global.LockClusterTimeout = 120
	c.Global.LockRefreshTime = 60
	return c
}

func TestFillDefaults(t *testing.T) {
	var c Configuration
	c.Pxcluster.PersistPrimarySettings = 5
	c.fillDefaults()
	if c.Pxcluster.MaintenanceHgRange != 9000 || c.Pxcluster.ConfigHgRange != 8000 || c.Pxcluster.PersistPrimarySettings != 0 {
		t.Errorf("fillDefaults() = %+v", c.Pxcluster)
	}
}

func TestGetConfig(t *testing.T) {
	for _, f := range []string{"config.toml", "config-cluster-example.toml", "config-daemon-example.toml"} {
		t.Run(f, func(t *testing.T) {
			c := GetConfig(filepath.Join("..", "..", "config", f))
			if c.Proxysql.Port == 0 || c.Pxcluster.HgW == 0 || c.Pxcluster.HgR == 0 {
				t.Errorf("GetConfig(%s) did not load core values: %+v", f, c)
			}
			if c.Pxcluster.MaintenanceHgRange == 0 || c.Pxcluster.ConfigHgRange == 0 {
				t.Errorf("GetConfig(%s) lost defaults: %+v", f, c.Pxcluster)
			}
		})
	}
}

func TestSanityCheck_Valid(t *testing.T) {
	c := validConfig(t)
	if !c.SanityCheck() {
		t.Fatal("SanityCheck() = false on valid config")
	}
	if c.Pxcluster.BckHgW != 8100 || c.Pxcluster.BckHgR != 8101 {
		t.Errorf("backup HGs = %d/%d, want 8100/8101", c.Pxcluster.BckHgW, c.Pxcluster.BckHgR)
	}
	if c.Global.LockRefreshTime != 60 {
		t.Errorf("LockRefreshTime changed to %d", c.Global.LockRefreshTime)
	}
}

func TestSanityCheck_Rejects(t *testing.T) {
	t.Run("single primary with many writers", func(t *testing.T) {
		c := validConfig(t)
		c.Pxcluster.MaxNumWriters = 2
		if c.SanityCheck() {
			t.Error("SanityCheck() = true, want false")
		}
	})
	t.Run("writer not reader with multiple writers", func(t *testing.T) {
		c := validConfig(t)
		c.Pxcluster.WriterIsAlsoReader = 0
		c.Pxcluster.MaxWriters = 2
		if c.SanityCheck() {
			t.Error("SanityCheck() = true, want false")
		}
	})
	t.Run("writer not reader without single primary", func(t *testing.T) {
		c := validConfig(t)
		c.Pxcluster.WriterIsAlsoReader = 0
		c.Pxcluster.SinglePrimary = false
		if c.SanityCheck() {
			t.Error("SanityCheck() = true, want false")
		}
	})
}

func TestSanityCheck_Adjustments(t *testing.T) {
	t.Run("deprecated backup HGs recomputed", func(t *testing.T) {
		c := validConfig(t)
		c.Pxcluster.BckHgW = 1
		c.Pxcluster.BckHgR = 2
		c.SanityCheck()
		if c.Pxcluster.BckHgW != 8100 || c.Pxcluster.BckHgR != 8101 {
			t.Errorf("backup HGs = %d/%d, want 8100/8101", c.Pxcluster.BckHgW, c.Pxcluster.BckHgR)
		}
	})
	t.Run("zero ping timeouts default to 1000", func(t *testing.T) {
		c := validConfig(t)
		c.Proxysql.PingTimeout = 0
		c.Pxcluster.PingTimeout = 0
		c.SanityCheck()
		if c.Proxysql.PingTimeout != 1000 || c.Pxcluster.PingTimeout != 1000 {
			t.Errorf("ping timeouts = %d/%d", c.Proxysql.PingTimeout, c.Pxcluster.PingTimeout)
		}
	})
	t.Run("ping timeouts capped to half check timeout", func(t *testing.T) {
		c := validConfig(t)
		c.Pxcluster.CheckTimeOut = 800
		c.SanityCheck()
		if c.Proxysql.PingTimeout != 400 || c.Pxcluster.PingTimeout != 400 {
			t.Errorf("ping timeouts = %d/%d, want 400/400", c.Proxysql.PingTimeout, c.Pxcluster.PingTimeout)
		}
	})
	t.Run("empty lock path defaults to /tmp", func(t *testing.T) {
		c := validConfig(t)
		c.Proxysql.LockFilePath = ""
		if !c.SanityCheck() {
			t.Error("SanityCheck() = false")
		}
		if c.Proxysql.LockFilePath != "/tmp" {
			t.Errorf("LockFilePath = %q", c.Proxysql.LockFilePath)
		}
	})
	t.Run("lock cluster timeout raised to minimum", func(t *testing.T) {
		c := validConfig(t)
		c.Global.LockClusterTimeout = 10
		c.Global.LockRefreshTime = 0
		c.SanityCheck()
		if c.Global.LockClusterTimeout != 60 {
			t.Errorf("LockClusterTimeout = %d, want 60", c.Global.LockClusterTimeout)
		}
		if c.Global.LockRefreshTime != 44 {
			t.Errorf("LockRefreshTime = %d, want 44", c.Global.LockRefreshTime)
		}
	})
	t.Run("lock refresh above 3/4 of timeout adjusted", func(t *testing.T) {
		c := validConfig(t)
		c.Global.LockRefreshTime = 100
		c.SanityCheck()
		if c.Global.LockRefreshTime != 89 {
			t.Errorf("LockRefreshTime = %d, want 89", c.Global.LockRefreshTime)
		}
	})
	t.Run("ssl paths missing still valid", func(t *testing.T) {
		c := validConfig(t)
		c.Pxcluster.SslcertificatePath = filepath.Join(t.TempDir(), "nope")
		if !c.SanityCheck() {
			t.Error("SanityCheck() = false")
		}
	})
	t.Run("ssl dir present but files missing still valid", func(t *testing.T) {
		c := validConfig(t)
		c.Pxcluster.SslcertificatePath = t.TempDir()
		c.Pxcluster.SslCa = "ca.pem"
		c.Pxcluster.SslKey = "key.pem"
		c.Pxcluster.SslClient = "client.pem"
		if !c.SanityCheck() {
			t.Error("SanityCheck() = false")
		}
	})
}

func TestInitLog(t *testing.T) {
	origLevel := log.GetLevel()
	defer func() {
		log.SetOutput(os.Stderr)
		log.SetLevel(origLevel)
	}()

	levels := map[string]log.Level{
		"debug":   log.DebugLevel,
		"info":    log.InfoLevel,
		"warning": log.WarnLevel,
		"error":   log.ErrorLevel,
		"bogus":   log.ErrorLevel,
	}
	for name, want := range levels {
		t.Run("level "+name, func(t *testing.T) {
			var c Configuration
			c.Global.LogTarget = "file"
			c.Global.LogFile = filepath.Join(t.TempDir(), "test.log")
			c.Global.LogLevel = name
			if !InitLog(c) {
				t.Fatal("InitLog() = false")
			}
			if log.GetLevel() != want {
				t.Errorf("level = %v, want %v", log.GetLevel(), want)
			}
		})
	}

	t.Run("stdout", func(t *testing.T) {
		var c Configuration
		c.Global.LogTarget = "STDOUT"
		c.Global.LogLevel = "error"
		if !InitLog(c) {
			t.Error("InitLog() = false")
		}
	})

	t.Run("unwritable file", func(t *testing.T) {
		var c Configuration
		c.Global.LogTarget = "file"
		c.Global.LogFile = filepath.Join(t.TempDir(), "missing-dir", "test.log")
		log.SetOutput(os.Stderr)
		if InitLog(c) {
			t.Error("InitLog() = true for unwritable file, want false")
		}
	})
}

func TestLogFormat(t *testing.T) {
	f := &LogFormat{TimestampFormat: "2006-01-02"}
	entry := &log.Entry{
		Level:   log.WarnLevel,
		Time:    time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
		Message: "hello",
		Data:    log.Fields{"node": "n1"},
	}
	out, err := f.Format(entry)
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	for _, want := range []string{"\x1b[33m", "[WARNING]", "2024-03-01", " - hello", " || ", "node={n1}", "\n"} {
		if !strings.Contains(s, want) {
			t.Errorf("Format() = %q, missing %q", s, want)
		}
	}

	bare, _ := f.Format(&log.Entry{Level: log.InfoLevel, Time: entry.Time})
	if strings.Contains(string(bare), " - ") || strings.Contains(string(bare), " || ") {
		t.Errorf("Format() of empty entry = %q", bare)
	}
}

func TestGetColorByLevel(t *testing.T) {
	tests := map[log.Level]int{
		log.DebugLevel: colorGray,
		log.WarnLevel:  colorYellow,
		log.ErrorLevel: colorRed,
		log.PanicLevel: paniclevel,
		log.FatalLevel: paniclevel,
		log.InfoLevel:  colorBlue,
		log.TraceLevel: colorBlue,
	}
	for lvl, want := range tests {
		if got := getColorByLevel(lvl); got != want {
			t.Errorf("getColorByLevel(%v) = %d, want %d", lvl, got, want)
		}
	}
}

func TestHelpText(t *testing.T) {
	var h HelpText
	h.Init()
	if h.inParams != [2]string{"configfile", "configPath"} {
		t.Errorf("Init() inParams = %v", h.inParams)
	}
	text := h.GetHelpText()
	for _, want := range []string{"pxcScheduler", "--configfile", "ProxySQL"} {
		if !strings.Contains(text, want) {
			t.Errorf("GetHelpText() missing %q", want)
		}
	}
}

// ****************  property-based tests (hegel) **********************************

// drawConfig draws a configuration across the full range of values SanityCheck inspects.
func drawConfig(ht *hegel.T, lockDir string) Configuration {
	var c Configuration
	c.fillDefaults()
	c.Pxcluster.SinglePrimary = hegel.Draw(ht, hegel.Booleans())
	c.Pxcluster.MaxNumWriters = hegel.Draw(ht, hegel.Integers(0, 5))
	c.Pxcluster.MaxWriters = hegel.Draw(ht, hegel.Integers(0, 5))
	c.Pxcluster.WriterIsAlsoReader = hegel.Draw(ht, hegel.Integers(0, 2))
	c.Pxcluster.HgW = hegel.Draw(ht, hegel.Integers(0, 1000))
	c.Pxcluster.HgR = hegel.Draw(ht, hegel.Integers(0, 1000))
	c.Pxcluster.ConfigHgRange = hegel.Draw(ht, hegel.Integers(0, 9000))
	c.Pxcluster.BckHgW = hegel.Draw(ht, hegel.Integers(0, 10000))
	c.Pxcluster.BckHgR = hegel.Draw(ht, hegel.Integers(0, 10000))
	c.Pxcluster.CheckTimeOut = hegel.Draw(ht, hegel.Integers(0, 10000))
	c.Pxcluster.PingTimeout = hegel.Draw(ht, hegel.Integers(0, 10000))
	c.Proxysql.PingTimeout = hegel.Draw(ht, hegel.Integers(0, 10000))
	c.Proxysql.LockFilePath = hegel.Draw(ht, hegel.SampledFrom([]string{"", lockDir}))
	c.Pxcluster.SslcertificatePath = hegel.Draw(ht, hegel.SampledFrom([]string{"", lockDir, filepath.Join(lockDir, "missing")}))
	c.Pxcluster.SslCa = hegel.Draw(ht, hegel.SampledFrom([]string{"", "ca.pem"}))
	c.Global.LockClusterTimeout = hegel.Draw(ht, hegel.Integers[int64](-100, 100000))
	c.Global.LockRefreshTime = hegel.Draw(ht, hegel.Integers[int64](-100, 100000))
	return c
}

// SinglePrimary with more than one writer is always rejected.
func TestProperty_SanityCheck_RejectsSinglePrimaryMultiWriter(t *testing.T) {
	lockDir := t.TempDir()
	hegel.Test(t, func(ht *hegel.T) {
		c := drawConfig(ht, lockDir)
		c.Pxcluster.SinglePrimary = true
		c.Pxcluster.MaxNumWriters = hegel.Draw(ht, hegel.Integers(2, 100))
		if c.SanityCheck() {
			ht.Fatalf("SanityCheck() accepted SinglePrimary with MaxNumWriters=%d", c.Pxcluster.MaxNumWriters)
		}
	}, hegel.WithTestCases(500))
}

// WriterIsAlsoReader other than 1 combined with more than one writer is always rejected.
func TestProperty_SanityCheck_RejectsWriterNotReaderMultiWriter(t *testing.T) {
	lockDir := t.TempDir()
	hegel.Test(t, func(ht *hegel.T) {
		c := drawConfig(ht, lockDir)
		ht.Assume(c.Pxcluster.WriterIsAlsoReader != 1 && (c.Pxcluster.MaxWriters > 1 || !c.Pxcluster.SinglePrimary))
		if c.SanityCheck() {
			ht.Fatalf("SanityCheck() accepted WriterIsAlsoReader=%d with MaxWriters=%d SinglePrimary=%v",
				c.Pxcluster.WriterIsAlsoReader, c.Pxcluster.MaxWriters, c.Pxcluster.SinglePrimary)
		}
	}, hegel.WithTestCases(500))
}

// Running SanityCheck on an already checked configuration changes nothing.
func TestProperty_SanityCheck_Idempotent(t *testing.T) {
	lockDir := t.TempDir()
	hegel.Test(t, func(ht *hegel.T) {
		c := drawConfig(ht, lockDir)
		ht.Assume(c.SanityCheck())
		once := c
		if !c.SanityCheck() {
			ht.Fatalf("second SanityCheck() rejected a config it accepted: %+v", once)
		}
		if !reflect.DeepEqual(once, c) {
			ht.Fatalf("second SanityCheck() changed config:\n once: %+v\ntwice: %+v", once, c)
		}
	}, hegel.WithTestCases(1000))
}

// Backup hostgroups are always derived from ConfigHgRange, whatever deprecated values were set.
func TestProperty_SanityCheck_BackupHostgroups(t *testing.T) {
	lockDir := t.TempDir()
	hegel.Test(t, func(ht *hegel.T) {
		c := drawConfig(ht, lockDir)
		ht.Assume(c.SanityCheck())
		if c.Pxcluster.BckHgW != c.Pxcluster.ConfigHgRange+c.Pxcluster.HgW ||
			c.Pxcluster.BckHgR != c.Pxcluster.ConfigHgRange+c.Pxcluster.HgR {
			ht.Fatalf("backup HGs %d/%d, want %d/%d", c.Pxcluster.BckHgW, c.Pxcluster.BckHgR,
				c.Pxcluster.ConfigHgRange+c.Pxcluster.HgW, c.Pxcluster.ConfigHgRange+c.Pxcluster.HgR)
		}
	}, hegel.WithTestCases(1000))
}

// An accepted config has LockClusterTimeout >= 60 and a positive refresh time within 3/4 of it.
func TestProperty_SanityCheck_LockTimes(t *testing.T) {
	lockDir := t.TempDir()
	hegel.Test(t, func(ht *hegel.T) {
		c := drawConfig(ht, lockDir)
		ht.Assume(c.SanityCheck())
		cto := c.Global.LockClusterTimeout
		rf := c.Global.LockRefreshTime
		if cto < 60 {
			ht.Fatalf("LockClusterTimeout = %d, want >= 60", cto)
		}
		if rf <= 0 || rf > int64(float64(cto)*0.75) {
			ht.Fatalf("LockRefreshTime = %d, want in (0, %d]", rf, int64(float64(cto)*0.75))
		}
	}, hegel.WithTestCases(1000))
}

// An accepted config has ping timeouts that are positive and shorter than CheckTimeOut.
func TestProperty_SanityCheck_PingTimeouts(t *testing.T) {
	lockDir := t.TempDir()
	hegel.Test(t, func(ht *hegel.T) {
		c := drawConfig(ht, lockDir)
		ht.Assume(c.SanityCheck())
		for name, p := range map[string]int{"proxysql": c.Proxysql.PingTimeout, "pxccluster": c.Pxcluster.PingTimeout} {
			if p <= 0 || p >= c.Pxcluster.CheckTimeOut {
				ht.Fatalf("%s PingTimeout = %d with CheckTimeOut = %d, want 0 < ping < CheckTimeOut", name, p, c.Pxcluster.CheckTimeOut)
			}
		}
	}, hegel.WithTestCases(1000))
}
