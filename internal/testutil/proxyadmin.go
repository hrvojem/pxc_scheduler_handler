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

// Package testutil holds test helpers shared by several packages. Only tests import it.
package testutil

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	_ "modernc.org/sqlite"
)

// ****************  ProxySQL admin stand-in **********************************

// ProxySQL keeps mysql_servers in SQLite, so an SQLite database with the same table definition
// runs the scheduler's SQL the way the admin interface does. LOAD/SAVE are admin-only commands
// and are accepted as no-ops.
const MysqlServersDDL = `CREATE TABLE mysql_servers (
	hostgroup_id INT CHECK (hostgroup_id>=0) NOT NULL DEFAULT 0,
	hostname VARCHAR NOT NULL,
	port INT CHECK (port >= 0 AND port <= 65535) NOT NULL DEFAULT 3306,
	gtid_port INT CHECK ((gtid_port <> port OR gtid_port=0) AND gtid_port >= 0 AND gtid_port <= 65535) NOT NULL DEFAULT 0,
	status VARCHAR CHECK (UPPER(status) IN ('ONLINE','SHUNNED','OFFLINE_SOFT', 'OFFLINE_HARD')) NOT NULL DEFAULT 'ONLINE',
	weight INT CHECK (weight >= 0 AND weight <=10000000) NOT NULL DEFAULT 1,
	compression INT CHECK (compression IN(0,1)) NOT NULL DEFAULT 0,
	max_connections INT CHECK (max_connections >=0) NOT NULL DEFAULT 1000,
	max_replication_lag INT CHECK (max_replication_lag >= 0 AND max_replication_lag <= 126144000) NOT NULL DEFAULT 0,
	use_ssl INT CHECK (use_ssl IN(0,1)) NOT NULL DEFAULT 0,
	max_latency_ms INT UNSIGNED CHECK (max_latency_ms>=0) NOT NULL DEFAULT 0,
	comment VARCHAR NOT NULL DEFAULT '',
	PRIMARY KEY (hostgroup_id, hostname, port))`

// AdminConnector opens SQLite connections that record every statement and skip LOAD/SAVE.
type AdminConnector struct {
	base driver.Driver
	dsn  string
	mu   sync.Mutex
	log  []string
}

func (c *AdminConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.base.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &adminConn{Conn: conn, owner: c}, nil
}

func (c *AdminConnector) Driver() driver.Driver { return c.base }

func (c *AdminConnector) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.log = nil
}

// Statements returns the recorded statements, without the LOAD/SAVE admin commands.
func (c *AdminConnector) Statements() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, s := range c.log {
		if !isAdminCommand(s) {
			out = append(out, s)
		}
	}
	return out
}

func isAdminCommand(query string) bool {
	upper := strings.ToUpper(strings.TrimSpace(query))
	return strings.HasPrefix(upper, "LOAD ") || strings.HasPrefix(upper, "SAVE ")
}

type adminConn struct {
	driver.Conn
	owner *AdminConnector
}

func (c *adminConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.owner.mu.Lock()
	c.owner.log = append(c.owner.log, query)
	c.owner.mu.Unlock()
	if isAdminCommand(query) {
		return driver.RowsAffected(0), nil
	}
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func (c *adminConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func (c *adminConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}

// NewAdminDB returns a database with an empty mysql_servers table and its statement recorder.
func NewAdminDB(t testing.TB) (*sql.DB, *AdminConnector) {
	probe, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	base := probe.Driver()
	probe.Close()

	// durability does not matter here; skipping fsync keeps the random-cluster tests fast
	connector := &AdminConnector{base: base, dsn: filepath.Join(t.TempDir(), "admin.db") + "?_pragma=synchronous(OFF)&_pragma=journal_mode(MEMORY)"}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(MysqlServersDDL); err != nil {
		t.Fatal(err)
	}
	return db, connector
}
