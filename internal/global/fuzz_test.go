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
	"strings"
	"testing"

	"github.com/Tusamarco/toml"
)

// ****************  fuzz targets (go test -fuzz) **********************************

// Any wsrep_provider_options text parses without panicking into trimmed, non-empty pairs.
func FuzzFromStringToMAp(f *testing.F) {
	f.Add("gmcast.segment = 0; pc.weight = 1", ";")
	f.Add("evs.keepalive_period = PT1S; socket.ssl_cipher = AES128-SHA256", ";")
	f.Add("a=1,b=2", ",")
	f.Fuzz(func(t *testing.T, input string, separator string) {
		for k, v := range FromStringToMAp(input, separator) {
			if k == "" || v == "" || k != strings.TrimSpace(k) || v != strings.TrimSpace(v) {
				t.Fatalf("FromStringToMAp(%q, %q) produced bad pair %q=%q", input, separator, k, v)
			}
		}
	})
}

// Any configuration file that decodes can be checked without panicking, and an accepted
// configuration has usable lock and ping settings.
func FuzzConfigSanityCheck(f *testing.F) {
	for _, name := range []string{"config.toml", "config-cluster-example.toml", "config-daemon-example.toml"} {
		if data, err := os.ReadFile(filepath.Join("..", "..", "config", name)); err == nil {
			f.Add(string(data))
		}
	}
	lockDir := f.TempDir()
	f.Fuzz(func(t *testing.T, text string) {
		// report a panic as a failure so it does not abort the rest of the package's tests
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("decoding or checking %q panicked: %v", text, r)
			}
		}()
		var c Configuration
		c.fillDefaults()
		if _, err := toml.Decode(text, &c); err != nil {
			return
		}
		// keep the checks on the local file system inside the test directory
		c.Proxysql.LockFilePath = lockDir
		c.Pxcluster.SslcertificatePath = ""
		if !c.SanityCheck() {
			return
		}
		if c.Global.LockClusterTimeout < 60 {
			t.Fatalf("accepted LockClusterTimeout = %d", c.Global.LockClusterTimeout)
		}
		if c.Pxcluster.MaxNumWriters > 1 && c.Pxcluster.SinglePrimary {
			t.Fatalf("accepted SinglePrimary with MaxNumWriters = %d", c.Pxcluster.MaxNumWriters)
		}
	})
}
