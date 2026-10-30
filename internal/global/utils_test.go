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
	"sort"
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"hegel.dev/go/hegel"
)

func TestFromStringToMAp(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		separator string
		want      map[string]string
	}{
		{"empty string", "", ",", map[string]string{}},
		{"empty separator", "a=1", "", map[string]string{}},
		{"single pair", "a=1", ",", map[string]string{"a": "1"}},
		{"multiple pairs trimmed", " a = 1 , b=2 ", ",", map[string]string{"a": "1", "b": "2"}},
		{"missing value skipped", "a=,b=2", ",", map[string]string{"b": "2"}},
		{"missing key skipped", "=1,b=2", ",", map[string]string{"b": "2"}},
		{"no equal sign skipped", "a,b=2", ",", map[string]string{"b": "2"}},
		{"semicolon separator", "x=10;y=20", ";", map[string]string{"x": "10", "y": "20"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FromStringToMAp(tt.input, tt.separator); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FromStringToMAp(%q, %q) = %v, want %v", tt.input, tt.separator, got, tt.want)
			}
		})
	}
}

func TestToInt(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"", 0},
		{"0", 0},
		{"42", 42},
		{"-7", -7},
		{"abc", -1},
		{"1.5", -1},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := ToInt(tt.input); got != tt.want {
				t.Errorf("ToInt(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestToBool(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		trueValue string
		want      bool
	}{
		{"exact match", "ON", "ON", true},
		{"case insensitive", "on", "ON", true},
		{"mismatch", "OFF", "ON", false},
		{"empty input", "", "", false},
		{"empty true string", "on", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ToBool(tt.input, tt.trueValue); got != tt.want {
				t.Errorf("ToBool(%q, %q) = %v, want %v", tt.input, tt.trueValue, got, tt.want)
			}
		})
	}
}

func TestBool2int(t *testing.T) {
	if Bool2int(true) != 1 {
		t.Error("Bool2int(true) != 1")
	}
	if Bool2int(false) != 0 {
		t.Error("Bool2int(false) != 0")
	}
}

func TestRemoveIndexAndChompSlice(t *testing.T) {
	if got := RemoveIndex([]string{"a", "b", "c"}, 1); !reflect.DeepEqual(got, []string{"a", "c"}) {
		t.Errorf("RemoveIndex() = %v", got)
	}
	if got := RemoveIndex([]string{"a", "b", "c"}, 2); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Errorf("RemoveIndex() last = %v", got)
	}
	if got := ChompSlice([]string{"a", "b", "c"}, 2); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Errorf("ChompSlice() = %v", got)
	}
	if got := ChompSlice([]string{"a", "b", "c"}, 0); len(got) != 0 {
		t.Errorf("ChompSlice() to zero = %v", got)
	}
}

func TestReflectStructField(t *testing.T) {
	type sample struct{ Name string }
	if err := ReflectStructField(&sample{}, "Name"); err != nil {
		t.Errorf("pointer with existing field: unexpected error %v", err)
	}
	if err := ReflectStructField(sample{}, "Name"); err != nil {
		t.Errorf("value with existing field: unexpected error %v", err)
	}
	if err := ReflectStructField(&sample{}, "Missing"); err == nil {
		t.Error("missing field: expected error, got nil")
	}
}

func TestCaseInsensitiveFieldByName(t *testing.T) {
	type sample struct{ HostName string }
	v := reflect.ValueOf(sample{HostName: "node1"})
	if f := caseInsenstiveFieldByName(v, "hostname"); !f.IsValid() || f.String() != "node1" {
		t.Errorf("caseInsenstiveFieldByName() = %v", f)
	}
	if f := caseInsenstiveFieldByName(v, "nope"); f.IsValid() {
		t.Error("expected invalid value for missing field")
	}
}

func TestCheckIfPathExists(t *testing.T) {
	dir := t.TempDir()
	if !CheckIfPathExists(dir) {
		t.Errorf("CheckIfPathExists(%q) = false, want true", dir)
	}
	if CheckIfPathExists(filepath.Join(dir, "does-not-exist")) {
		t.Error("CheckIfPathExists() on missing path = true, want false")
	}
}

func TestMyWaitGroupCounter(t *testing.T) {
	var wg MyWaitGroup
	wg.DecreaseCounter() // must not go negative
	if got := wg.ReportCounter(); got != 0 {
		t.Fatalf("ReportCounter() = %d, want 0", got)
	}
	wg.IncreaseCounter()
	wg.IncreaseCounter()
	wg.DecreaseCounter()
	if got := wg.ReportCounter(); got != 1 {
		t.Errorf("ReportCounter() = %d, want 1", got)
	}
}

func TestMyWaitGroupWaitTimeout(t *testing.T) {
	var done MyWaitGroup
	if done.WaitTimeout(time.Second) {
		t.Error("WaitTimeout() on empty group = true (timed out), want false")
	}

	var pending MyWaitGroup
	pending.Add(1)
	defer pending.Done()
	if !pending.WaitTimeout(10 * time.Millisecond) {
		t.Error("WaitTimeout() on pending group = false, want true (timed out)")
	}
}

func TestStatSyncInfo(t *testing.T) {
	m := NewRegularIntMap()
	if _, ok := m.Load("k"); ok {
		t.Fatal("Load() on empty map returned ok")
	}
	m.Store("k", [2]int64{1, 2})
	if v, ok := m.Load("k"); !ok || v != [2]int64{1, 2} {
		t.Errorf("Load() = %v, %v", v, ok)
	}
	if v := m.get("k"); v != [2]int64{1, 2} {
		t.Errorf("get() = %v", v)
	}
	if len(m.ExposeMap()) != 1 {
		t.Errorf("ExposeMap() len = %d, want 1", len(m.ExposeMap()))
	}
	m.Delete("k")
	if _, ok := m.Load("k"); ok {
		t.Error("Load() after Delete() returned ok")
	}
}

func TestOrderedPerfMap(t *testing.T) {
	m := NewOrderedMap()
	m.Set("a", PerfObject{Name: "a"})
	m.Set("b", PerfObject{Name: "b"})
	m.Set("c", PerfObject{Name: "c"})
	m.Set("a", PerfObject{Name: "a2"}) // overwrite must keep original position

	if v, ok := m.Get("a"); !ok || v.Name != "a2" {
		t.Errorf("Get(a) = %v, %v", v, ok)
	}
	if _, ok := m.Get("zzz"); ok {
		t.Error("Get() on missing key returned ok")
	}

	m.Delete("b")
	m.Delete("missing") // no-op

	var order []string
	it := m.Iterator()
	for {
		i, key, _ := it()
		if i == nil {
			break
		}
		order = append(order, *key)
	}
	if !reflect.DeepEqual(order, []string{"a", "c"}) {
		t.Errorf("iteration order = %v, want [a c]", order)
	}
}

func TestPerformanceTracking(t *testing.T) {
	PerformanceMap = NewRegularIntMap()
	PerformanceMapOrdered = NewOrderedMap()

	SetPerformanceValue("phase", true)
	SetPerformanceValue("phase", false)
	v, ok := PerformanceMap.Load("phase")
	if !ok || v[0] == 0 || v[1] < v[0] {
		t.Errorf("SetPerformanceValue() stored %v, %v", v, ok)
	}

	SetPerformanceObj("obj", true, log.InfoLevel)
	SetPerformanceObj("obj", false, log.InfoLevel)
	obj, ok := PerformanceMapOrdered.Get("obj")
	if !ok || obj.Name != "obj" || obj.LogLevel != log.InfoLevel || obj.Time[0] == 0 || obj.Time[1] < obj.Time[0] {
		t.Errorf("SetPerformanceObj() stored %+v, %v", obj, ok)
	}

	original := log.GetLevel()
	defer log.SetLevel(original)
	log.SetLevel(log.InfoLevel)
	ReportPerformance() // must terminate and not panic
}

func TestMain(m *testing.M) {
	log.SetOutput(os.Stderr)
	log.SetLevel(log.PanicLevel)
	os.Exit(m.Run())
}

// ****************  property-based tests (hegel) **********************************

// keyValueText draws a non-empty token that cannot contain the key/value
// delimiter, a pair separator, or whitespace/control characters (which are trimmed).
func keyValueText() hegel.Generator[string] {
	return hegel.Text().ExcludeCategories([]string{"Z", "Cc"}).ExcludeCharacters("=;,\u0085").MinSize(1).MaxSize(20)
}

// joinPairs renders a map as "k = v" pairs with random padding around tokens.
func joinPairs(ht *hegel.T, m map[string]string, separator string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	pad := hegel.SampledFrom([]string{"", " ", "  ", "\t"})
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, hegel.Draw(ht, pad)+k+hegel.Draw(ht, pad)+"="+hegel.Draw(ht, pad)+m[k]+hegel.Draw(ht, pad))
	}
	return strings.Join(parts, separator)
}

// Parsing a rendered key/value list (e.g. wsrep_provider_options) gives back the original map.
func TestProperty_FromStringToMAp_RoundTrip(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		separator := hegel.Draw(ht, hegel.SampledFrom([]string{";", ","}))
		want := hegel.Draw(ht, hegel.Maps(keyValueText(), keyValueText()).MaxSize(10))
		input := joinPairs(ht, want, separator)

		if got := FromStringToMAp(input, separator); !reflect.DeepEqual(got, want) {
			ht.Fatalf("FromStringToMAp(%q, %q) = %v, want %v", input, separator, got, want)
		}
	}, hegel.WithTestCases(1000))
}

// A value that itself contains '=' must survive parsing intact rather than being truncated.
func TestProperty_FromStringToMAp_ValueWithEquals(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		key := hegel.Draw(ht, keyValueText())
		left := hegel.Draw(ht, keyValueText())
		right := hegel.Draw(ht, keyValueText())
		want := map[string]string{key: left + "=" + right}
		input := key + " = " + left + "=" + right

		if got := FromStringToMAp(input, ";"); !reflect.DeepEqual(got, want) {
			ht.Fatalf("FromStringToMAp(%q) = %v, want %v", input, got, want)
		}
	}, hegel.WithTestCases(500))
}

// Arbitrary input never panics and only yields trimmed, non-empty keys and values.
func TestProperty_FromStringToMAp_AnyInput(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		input := hegel.Draw(ht, hegel.Text().MaxSize(200))
		separator := hegel.Draw(ht, hegel.Text().MaxSize(3))

		for k, v := range FromStringToMAp(input, separator) {
			if k == "" || v == "" || k != strings.TrimSpace(k) || v != strings.TrimSpace(v) {
				ht.Fatalf("FromStringToMAp(%q, %q) produced bad pair %q=%q", input, separator, k, v)
			}
		}
	}, hegel.WithTestCases(1000))
}
