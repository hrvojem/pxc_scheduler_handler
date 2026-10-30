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
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"pxc_scheduler_handler/internal/cluster"
	global "pxc_scheduler_handler/internal/global"
	SQLProxy "pxc_scheduler_handler/internal/query/proxysql"
	"pxc_scheduler_handler/internal/testutil"

	log "github.com/sirupsen/logrus"
	"hegel.dev/go/hegel"
)

func TestMain(m *testing.M) {
	log.SetLevel(log.PanicLevel)
	os.Exit(m.Run())
}

// The tests here drive ProxySQLNodeImpl with clusters built in memory. The cluster types live in
// the cluster package; the aliases keep the test code readable.
type (
	DataNodeImpl    = cluster.DataNodeImpl
	DataClusterImpl = cluster.DataClusterImpl
	Hostgroup       = cluster.Hostgroup
)

func newAdminDB(t *testing.T) (*sql.DB, *testutil.AdminConnector) { return testutil.NewAdminDB(t) }

// ****************  mirrors of unexported cluster code **********************************

// The functions below copy what the cluster package does internally when it loads a cluster from
// ProxySQL and from the PXC nodes. They are not reachable from this package, so they are repeated
// here. Keep them in step with internal/cluster/cluster.go.

// setParameters mirrors DataNodeImpl.setParameters.
func setParameters(node *DataNodeImpl) {
	node.WsrepLocalIndex = node.PxcView.LocalIndex
	node.PxcMaintMode = node.Variables["pxc_maint_mode"]
	node.WsrepConnected = global.ToBool(node.Status["wsrep_connected"], "ON")
	node.WsrepDesinccount = global.ToInt(node.Status["wsrep_desync_count"])
	node.WsrepDonorrejectqueries = global.ToBool(node.Variables["wsrep_sst_donor_rejects_queries"], "ON")
	node.WsrepGcommUuid = node.Status["wsrep_gcomm_uuid"]
	node.WsrepProvider = global.FromStringToMAp(node.Variables["wsrep_provider_options"], ";")
	node.HasPrimaryState = global.ToBool(node.Status["wsrep_cluster_status"], "Primary")

	node.WsrepClusterName = node.Variables["wsrep_cluster_name"]
	node.WsrepClusterStatus = node.Status["wsrep_cluster_status"]
	node.WsrepLocalRecvQueue = global.ToInt(node.Status["wsrep_local_recv_queue"])
	node.WsrepNodeName = node.Variables["wsrep_node_name"]
	node.WsrepClusterSize = global.ToInt(node.Status["wsrep_cluster_size"])
	node.WsrepPcWeight = global.ToInt(node.WsrepProvider["pc.weight"])
	node.WsrepReady = global.ToBool(node.Status["wsrep_ready"], "on")
	node.WsrepRejectqueries = !global.ToBool(node.Variables["wsrep_reject_queries"], "none")
	node.WsrepSegment = global.ToInt(node.WsrepProvider["gmcast.segment"])
	node.WsrepStatus = global.ToInt(node.Status["wsrep_local_state"])
	node.ReadOnly = global.ToBool(node.Variables["read_only"], "on")
}

// getRetry mirrors DataNodeImpl.getRetry.
func getRetry(node *DataNodeImpl, writeHG int, readHG int) {
	comment := node.Comment
	hgRetry := strconv.Itoa(writeHG) + "_W_" + strconv.Itoa(readHG) + "_R_retry_"
	up := regexp.MustCompile(hgRetry+"up=\\d;").FindAllString(comment, -1)
	down := regexp.MustCompile(hgRetry+"down=\\d;").FindAllString(comment, -1)
	if len(up) > 0 {
		comment = strings.ReplaceAll(comment, up[0], "")
		if v, err := strconv.Atoi(up[0][strings.Index(up[0], "=")+1 : len(up[0])-1]); err == nil {
			node.RetryUp = v
		}
	}
	if len(down) > 0 {
		comment = strings.ReplaceAll(comment, down[0], "")
		if v, err := strconv.Atoi(down[0][strings.Index(down[0], "=")+1 : len(down[0])-1]); err == nil {
			node.RetryDown = v
		}
	}
	node.Comment = comment
}

// getParametersFromProxySQL mirrors DataClusterImpl.getParametersFromProxySQL.
func getParametersFromProxySQL(c *DataClusterImpl, config global.Configuration) {
	c.ClusterIdentifier = config.Pxcluster.ClusterId
	c.HgWriterId = config.Pxcluster.HgW
	c.HgReaderId = config.Pxcluster.HgR
	c.BakcupHgWriterId = config.Pxcluster.BckHgW
	c.BackupHgReaderId = config.Pxcluster.BckHgR
	c.SinglePrimary = config.Pxcluster.SinglePrimary
	c.MaxNumWriters = config.Pxcluster.MaxNumWriters
	c.WriterIsReader = config.Pxcluster.WriterIsAlsoReader
	c.RetryUp = config.Pxcluster.RetryUp
	c.RetryDown = config.Pxcluster.RetryDown
	c.ConfigHgRange = config.Pxcluster.ConfigHgRange
	c.MaintenanceHgRange = config.Pxcluster.MaintenanceHgRange
	c.OffLineHgReaderID = c.MaintenanceHgRange + c.HgReaderId
	c.OffLineHgWriterId = c.MaintenanceHgRange + c.HgWriterId
}

// loadNodes mirrors DataClusterImpl.loadNodes, without the persist-primary handling the tests do not use.
func loadNodes(c *DataClusterImpl, config global.Configuration, db *sql.DB) error {
	hgs := []int{c.HgWriterId, c.HgReaderId, c.BakcupHgWriterId, c.BackupHgReaderId, c.OffLineHgWriterId, c.OffLineHgReaderID}
	var list []string
	for _, hg := range hgs {
		list = append(list, strconv.Itoa(hg))
	}

	c.ActionNodes = make(map[string]DataNodeImpl)
	c.NodesPxc = cluster.NewRegularIntMap()
	c.BackupWriters = make(map[string]DataNodeImpl)
	c.BackupReaders = make(map[string]DataNodeImpl)
	c.WriterNodes = make(map[string]DataNodeImpl)
	c.ReaderNodes = make(map[string]DataNodeImpl)
	c.OffLineWriters = make(map[string]DataNodeImpl)
	c.OffLineReaders = make(map[string]DataNodeImpl)

	rows, err := db.Query(strings.ReplaceAll(SQLProxy.Dml_Select_mysql_nodes, "?", strings.Join(list, ",")))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var n DataNodeImpl
		if err := rows.Scan(&n.HostgroupId, &n.Ip, &n.Port, &n.GtidPort, &n.ProxyStatus, &n.Weight, &n.Compression,
			&n.MaxConnection, &n.MaxReplicationLag, &n.UseSsl, &n.MaxLatency, &n.Comment, &n.ConnUsed); err != nil {
			return err
		}
		n.User = c.MonitorUser
		n.Password = c.MonitorPassword
		n.Dns = net.JoinHostPort(n.Ip, strconv.Itoa(n.Port))
		n.PingTimeout = config.Pxcluster.PingTimeout
		if len(n.Comment) > 0 {
			getRetry(&n, c.HgWriterId, c.HgReaderId)
		}
		if c.Ssl != nil {
			n.Ssl = c.Ssl
		}

		switch n.HostgroupId {
		case c.HgWriterId:
			c.WriterNodes[n.Dns] = n
		case c.HgReaderId:
			c.ReaderNodes[n.Dns] = n
		case c.BakcupHgWriterId:
			c.BackupWriters[n.Dns] = n
		case c.BackupHgReaderId:
			c.BackupReaders[n.Dns] = n
		case c.OffLineHgWriterId:
			c.OffLineWriters[n.Dns] = n
		case c.OffLineHgReaderID:
			c.OffLineReaders[n.Dns] = n
		}

		if _, ok := c.NodesPxc.Load(n.Dns); !ok {
			if n.HostgroupId == c.HgWriterId || n.HostgroupId == c.HgReaderId ||
				n.HostgroupId == c.OffLineHgWriterId || n.HostgroupId == c.OffLineHgReaderID {
				c.NodesPxc.Store(n.Dns, n)
			}
		}
	}
	return rows.Err()
}

// checkMissingForNodes mirrors DataClusterImpl.checkMissingForNodes, applied to NodesPxc.
func checkMissingForNodes(c *DataClusterImpl) {
	for _, m := range []map[string]DataNodeImpl{c.BackupReaders, c.BackupWriters} {
		for _, node := range m {
			if _, ok := c.NodesPxc.Load(node.Dns); !ok {
				node.HostgroupId = c.HgReaderId
				node.NodeIsNew = true
				c.NodesPxc.Store(node.Dns, node)
			}
		}
	}
}

// alignNodeValues mirrors DataClusterImpl.alignNodeValues.
func alignNodeValues(destination DataNodeImpl, source DataNodeImpl) DataNodeImpl {
	destination.ActionType = source.ActionType
	destination.Variables = source.Variables
	destination.Status = source.Status
	destination.PxcMaintMode = source.PxcMaintMode
	destination.PxcView = source.PxcView
	destination.Processed = source.Processed
	setParameters(&destination)
	return destination
}

// consolidateNodes mirrors DataClusterImpl.consolidateNodes.
func consolidateNodes(c *DataClusterImpl) {
	for key, node := range c.NodesPxc.ExposeMap() {
		nodeIsIn := false
		for _, m := range []map[string]DataNodeImpl{c.WriterNodes, c.ReaderNodes, c.OffLineWriters, c.OffLineReaders} {
			if _, ok := m[key]; ok {
				m[key] = alignNodeValues(m[key], node)
				nodeIsIn = true
			}
		}
		for _, m := range []map[string]DataNodeImpl{c.BackupWriters, c.BackupReaders} {
			if _, ok := m[key]; ok {
				m[key] = alignNodeValues(m[key], node)
			}
		}
		if node.HasPrimaryState {
			c.HasPrimary = true
			c.Size = node.WsrepClusterSize
			c.Name = node.WsrepClusterName
		}
		if !nodeIsIn {
			c.ReaderNodes[key] = node
		}
	}
}

// consolidateHGs mirrors DataClusterImpl.consolidateHGs.
func consolidateHGs(c *DataClusterImpl) {
	online := func(m map[string]DataNodeImpl) int {
		n := 0
		for _, node := range m {
			if node.ProxyStatus == "ONLINE" {
				n++
			}
		}
		return n
	}
	specialWrite := c.HgWriterId + c.MaintenanceHgRange
	specialRead := c.HgReaderId + c.MaintenanceHgRange
	c.Hostgroups = map[int]Hostgroup{
		c.HgWriterId: {Id: c.HgWriterId, Type: "W", Size: online(c.WriterNodes)},
		c.HgReaderId: {Id: c.HgReaderId, Type: "R", Size: online(c.ReaderNodes)},
		specialWrite: {Id: specialWrite, Type: "WREC", Size: online(c.OffLineWriters)},
		specialRead:  {Id: specialRead, Type: "RREC", Size: online(c.OffLineReaders)},
	}
}

// ****************  cluster builders **********************************

// newTestCluster returns a cluster with writer HG 100, reader HG 101,
// config HGs 8100/8101 and maintenance HGs 9100/9101.
func newTestCluster() *DataClusterImpl {
	return &DataClusterImpl{
		ActionNodes:        make(map[string]DataNodeImpl),
		BackupReaders:      make(map[string]DataNodeImpl),
		BackupWriters:      make(map[string]DataNodeImpl),
		BackupHgReaderId:   8101,
		BakcupHgWriterId:   8100,
		ConfigHgRange:      8000,
		MaintenanceHgRange: 9000,
		CheckTimeout:       60,
		ClusterIdentifier:  10,
		ClusterSize:        3,
		ClusterName:        "test",
		Comment:            "comment",
		Debug:              true,
		Haswriter:          true,
		HgReaderId:         101,
		HgWriterId:         100,
		Hostgroups: map[int]Hostgroup{
			100:  {Id: 100, Size: 1, Type: "W", Nodes: []DataNodeImpl{}},
			101:  {Id: 101, Size: 3, Type: "R", Nodes: []DataNodeImpl{}},
			9100: {Id: 9100, Size: 0, Type: "WREC", Nodes: []DataNodeImpl{}},
			9101: {Id: 9101, Size: 0, Type: "RREC", Nodes: []DataNodeImpl{}},
		},
		MainSegment:       1,
		MonitorPassword:   "password",
		MonitorUser:       "monitor",
		Name:              "test",
		NodesPxc:          cluster.NewRegularIntMap(),
		NodesPxcMaint:     []DataNodeImpl{},
		MaxNumWriters:     1,
		OffLineReaders:    make(map[string]DataNodeImpl),
		OffLineWriters:    make(map[string]DataNodeImpl),
		OffLineHgReaderID: 9101,
		OffLineHgWriterId: 9100,
		ReaderNodes:       make(map[string]DataNodeImpl),
		Singlenode:        true,
		SinglePrimary:     true,
		Size:              3,
		Ssl:               new(cluster.SslCertificates),
		Status:            4,
		WriterIsReader:    1,
		WriterNodes:       make(map[string]DataNodeImpl),
	}
}

// healthyNode builds a node whose derived wsrep fields come from realistic
// variables/status maps, the same way getInfo populates them.
func healthyNode(dns string, hg int, weight int) DataNodeImpl {
	node := DataNodeImpl{
		Comment:         "comment",
		Debug:           true,
		Dns:             dns,
		GtidPort:        33062,
		HostgroupId:     hg,
		Hostgroups:      []Hostgroup{},
		Ip:              "127.0.0.1",
		MaxConnection:   300,
		MaxLatency:      100,
		Name:            "testNode",
		Password:        "password",
		Port:            3306,
		Processed:       true,
		ProcessStatus:   1,
		ProxyStatus:     "ONLINE",
		Ssl:             new(cluster.SslCertificates),
		User:            "user",
		Weight:          weight,
		WsrepLocalIndex: 1,
	}
	node.Variables = map[string]string{
		"pxc_maint_mode":                  "DISABLED",
		"wsrep_sst_donor_rejects_queries": "OFF",
		"wsrep_provider_options":          "gmcast.segment = 1; pc.weight = 1",
		"wsrep_cluster_name":              "test",
		"wsrep_node_name":                 dns,
		"wsrep_reject_queries":            "NONE",
		"read_only":                       "OFF",
	}
	node.Status = map[string]string{
		"wsrep_connected":        "ON",
		"wsrep_desync_count":     "0",
		"wsrep_gcomm_uuid":       "uuid-" + dns,
		"wsrep_cluster_status":   "Primary",
		"wsrep_local_recv_queue": "0",
		"wsrep_cluster_size":     "3",
		"wsrep_ready":            "ON",
		"wsrep_local_state":      "4",
	}
	setParameters(&node)
	return node
}

type nodeSpec struct {
	weight      int
	segment     int
	localState  string
	readOnly    bool
	reject      bool
	maint       bool
	primary     bool
	processed   bool
	proxyStatus string
	writer      bool
	reader      bool
}

type clusterSpec struct {
	nodes          []nodeSpec
	maxNumWriters  int
	failBack       bool
	activeFailover int
	writerIsReader int
}

// drawNode draws one node; healthy draws every fault off and keeps it ONLINE and processed.
func drawNode(ht *hegel.T, healthy bool) nodeSpec {
	n := nodeSpec{
		weight:      hegel.Draw(ht, hegel.Integers(1, 1000)),
		segment:     hegel.Draw(ht, hegel.SampledFrom([]int{0, 0, 0, 1})),
		localState:  "4",
		primary:     true,
		processed:   true,
		proxyStatus: "ONLINE",
		reader:      true,
	}
	if !healthy {
		n.localState = hegel.Draw(ht, hegel.SampledFrom([]string{"4", "4", "4", "2", "1", "3"}))
		n.readOnly = hegel.Draw(ht, hegel.WeightedBooleans(0.2))
		n.reject = hegel.Draw(ht, hegel.WeightedBooleans(0.2))
		n.maint = hegel.Draw(ht, hegel.WeightedBooleans(0.2))
		n.primary = hegel.Draw(ht, hegel.WeightedBooleans(0.85))
		n.processed = hegel.Draw(ht, hegel.WeightedBooleans(0.9))
		n.proxyStatus = hegel.Draw(ht, hegel.SampledFrom([]string{"ONLINE", "ONLINE", "ONLINE", "OFFLINE_SOFT", "SHUNNED"}))
		n.reader = hegel.Draw(ht, hegel.WeightedBooleans(0.85))
	}
	return n
}

func drawClusterSpec(ht *hegel.T, healthy bool) clusterSpec {
	size := hegel.Draw(ht, hegel.Integers(1, 5))
	spec := clusterSpec{
		maxNumWriters:  hegel.Draw(ht, hegel.Integers(1, 2)),
		failBack:       hegel.Draw(ht, hegel.Booleans()),
		activeFailover: hegel.Draw(ht, hegel.Integers(0, 2)),
		writerIsReader: hegel.Draw(ht, hegel.Integers(0, 1)),
	}
	// SanityCheck rejects writerIsAlsoReader != 1 together with more than one writer
	if spec.maxNumWriters > 1 {
		spec.writerIsReader = 1
	}
	for i := 0; i < size; i++ {
		spec.nodes = append(spec.nodes, drawNode(ht, healthy))
	}
	// writers: a random subset, at most maxNumWriters+1 so over-provisioned clusters are covered too
	for i := range spec.nodes {
		spec.nodes[i].writer = hegel.Draw(ht, hegel.WeightedBooleans(0.4))
	}
	return spec
}

// String renders a spec as one line per node for failure messages.
func (spec clusterSpec) String() string {
	out := fmt.Sprintf(" cluster: maxNumWriters=%d failBack=%v activeFailover=%d writerIsReader=%d\n",
		spec.maxNumWriters, spec.failBack, spec.activeFailover, spec.writerIsReader)
	for i, n := range spec.nodes {
		out += fmt.Sprintf(" %s weight=%d seg=%d state=%s primary=%v read_only=%v reject=%v maint=%v processed=%v proxy=%s writer=%v reader=%v\n",
			specDns(i), n.weight, n.segment, n.localState, n.primary, n.readOnly, n.reject, n.maint, n.processed, n.proxyStatus, n.writer, n.reader)
	}
	return out
}

func specDns(i int) string { return "127.0.0.1:" + strconv.Itoa(43306+i) }

// buildCluster turns a spec into a cluster the same way loadNodes/getNodesInfo/consolidateHGs would.
func buildCluster(spec clusterSpec) *DataClusterImpl {
	cluster := newTestCluster()
	cluster.MaxNumWriters = spec.maxNumWriters
	cluster.SinglePrimary = spec.maxNumWriters == 1
	cluster.FailBack = spec.failBack
	cluster.ActiveFailover = spec.activeFailover
	cluster.WriterIsReader = spec.writerIsReader
	cluster.MainSegment = 0
	for i, s := range spec.nodes {
		dns := specDns(i)
		node := healthyNode(dns, cluster.HgReaderId, s.weight)
		node.Variables["wsrep_provider_options"] = "gmcast.segment = " + strconv.Itoa(s.segment) + "; pc.weight = 1"
		node.Variables["read_only"] = map[bool]string{true: "ON", false: "OFF"}[s.readOnly]
		node.Variables["wsrep_reject_queries"] = map[bool]string{true: "ALL", false: "NONE"}[s.reject]
		node.Variables["pxc_maint_mode"] = map[bool]string{true: "MAINTENANCE", false: "DISABLED"}[s.maint]
		node.Status["wsrep_local_state"] = s.localState
		node.Status["wsrep_cluster_status"] = map[bool]string{true: "Primary", false: "non-Primary"}[s.primary]
		setParameters(&node)
		node.Processed = s.processed
		node.ProxyStatus = s.proxyStatus
		cluster.NodesPxc.Store(dns, node)

		if s.reader {
			cluster.ReaderNodes[dns] = node
		}
		if s.writer {
			w := node
			w.HostgroupId = cluster.HgWriterId
			cluster.WriterNodes[dns] = w
		}
		bkW, bkR := node, node
		bkW.HostgroupId, bkR.HostgroupId = cluster.BakcupHgWriterId, cluster.BackupHgReaderId
		bkW.ProxyStatus, bkR.ProxyStatus = "ONLINE", "ONLINE"
		cluster.BackupWriters[dns] = bkW
		cluster.BackupReaders[dns] = bkR
	}
	consolidateHGs(cluster)
	return cluster
}
