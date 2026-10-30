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
	"strings"
	"testing"
)

// ****************  fuzz targets (go test -fuzz) **********************************

// The SQL that saves a node's retries runs for any comment the node already has.
func FuzzSaveRetrySQL(f *testing.F) {
	f.Add("comment")
	f.Add("Bob's node")
	f.Add(`back\slash`)
	f.Fuzz(func(t *testing.T, comment string) {
		if strings.ContainsRune(comment, 0) {
			return
		}
		db, _ := newAdminDB(t)
		proxy := newProxyNode(db, newTestCluster())
		node := healthyNode("10.0.0.1:3306", 101, 1)
		node.Comment = comment
		if err := insertServer(db, serverRow{hg: 101, host: "10.0.0.1", port: 3306, status: "ONLINE", weight: 1, maxConn: 1000, comment: comment}); err != nil {
			t.Fatal(err)
		}
		stmt := proxy.SaveRetry(node, 101, "10.0.0.1", 3306)
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("SaveRetry SQL failed for comment %q: %v\n%s", comment, err, stmt)
		}
		got, err := readComment(db, 101, "10.0.0.1", 3306)
		if err != nil || !strings.HasPrefix(got, comment) {
			t.Fatalf("comment after SaveRetry = %q (%v), want it to start with %q", got, err, comment)
		}
	})
}
