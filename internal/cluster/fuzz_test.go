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

package cluster

import (
	"strings"
	"testing"
)

// ****************  fuzz targets (go test -fuzz) **********************************

// Reading retries from any comment never panics and never makes the comment longer.
func FuzzGetRetry(f *testing.F) {
	f.Add("100_W_101_R_retry_up=1;100_W_101_R_retry_down=2;", 100, 101)
	f.Add("comment 100_W_101_R_retry_down=10;", 100, 101)
	f.Add("", 0, 0)
	f.Fuzz(func(t *testing.T, comment string, writeHG int, readHG int) {
		node := DataNodeImpl{Comment: comment}
		node.getRetry(writeHG, readHG)
		if len(node.Comment) > len(comment) {
			t.Fatalf("getRetry(%d, %d) grew comment %q to %q", writeHG, readHG, comment, node.Comment)
		}
		if node.RetryUp < 0 || node.RetryDown < 0 {
			t.Fatalf("getRetry(%d, %d) on %q gave negative retries %d/%d", writeHG, readHG, comment, node.RetryUp, node.RetryDown)
		}
	})
}

// Any SHOW VARIABLES / SHOW STATUS values are turned into node fields without panicking, and a
// node is only seen as synced and Primary when the server reported exactly that.
func FuzzSetParameters(f *testing.F) {
	f.Add("gmcast.segment = 0; pc.weight = 1", "4", "Primary", "OFF", "NONE", "DISABLED")
	f.Add("gmcast.segment = x", "Synced", "non-Primary", "ON", "ALL", "MAINTENANCE")
	f.Fuzz(func(t *testing.T, provider, localState, clusterStatus, readOnly, reject, maint string) {
		node := DataNodeImpl{
			Variables: map[string]string{
				"wsrep_provider_options": provider,
				"read_only":              readOnly,
				"wsrep_reject_queries":   reject,
				"pxc_maint_mode":         maint,
			},
			Status: map[string]string{
				"wsrep_local_state":    localState,
				"wsrep_cluster_status": clusterStatus,
			},
		}
		node.setParameters()
		if node.WsrepStatus == 4 && strings.TrimLeft(strings.TrimPrefix(localState, "+"), "0") != "4" {
			t.Fatalf("wsrep_local_state %q read as 4", localState)
		}
		if node.HasPrimaryState && !strings.EqualFold(clusterStatus, "Primary") {
			t.Fatalf("wsrep_cluster_status %q read as Primary", clusterStatus)
		}
	})
}
