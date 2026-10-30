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
	"os"
	"path/filepath"
	"testing"
)

// ****************  fuzz targets (go test -fuzz) **********************************

// Reading a lock file never panics, whatever its content.
func FuzzCheckLockFileExists(f *testing.F) {
	f.Add([]byte("PID:1234\nTime:1700000000000000000\n"))
	f.Add([]byte("PID:1234\n"))
	f.Add([]byte(""))
	dir := f.TempDir()
	f.Fuzz(func(t *testing.T, data []byte) {
		path := filepath.Join(dir, "lock")
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		defer os.Remove(path)
		// report a panic as a failure so it does not abort the rest of the package's tests
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("CheckLockFIleExists() panicked on %q: %v", data, r)
			}
		}()
		fl := FileLockImp{flPid: 1, flFullPath: path}
		fl.CheckLockFIleExists()
	})
}
