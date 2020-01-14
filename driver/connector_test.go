/*
Copyright 2014 SAP SE

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver_test

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"

	hdb "github.com/SAP/go-hdb/driver"
	"github.com/SAP/go-hdb/proxy"
	"github.com/stretchr/testify/require"
)

func TestConnector(t *testing.T) {
	dsnConnector, err := hdb.NewDSNConnector(hdb.TestDSN)
	if err != nil {
		t.Fatal(err)
	}
	testConnector(t, dsnConnector)

	basicAuthConnector := hdb.NewBasicAuthConnector(dsnConnector.Host(), dsnConnector.Username(), dsnConnector.Password())
	testConnector(t, basicAuthConnector)
}

func testConnector(t *testing.T, connector driver.Connector) {
	db := sql.OpenDB(connector)
	defer db.Close()

	var dummy string
	err := db.QueryRow("select * from dummy").Scan(&dummy)
	switch {
	case err == sql.ErrNoRows:
		t.Fatal(err)
	case err != nil:
		t.Fatal(err)
	}
	if dummy != "X" {
		t.Fatalf("dummy is %s - expected %s", dummy, "X")
	}
}

func TestSessionVariables(t *testing.T) {
	ctor, err := hdb.NewDSNConnector(hdb.TestDSN)
	if err != nil {
		t.Fatal(err)
	}
	// set session variables
	sv := hdb.SessionVariables{"k1": "v1", "k2": "v2", "k3": "v3"}
	if err := ctor.SetSessionVariables(sv); err != nil {
		t.Fatal(err)
	}

	// check session variables
	db := sql.OpenDB(ctor)
	defer db.Close()

	var val string
	for k, v := range sv {
		err := db.QueryRow(fmt.Sprintf("select session_context('%s') from dummy", k)).Scan(&val)
		switch {
		case err == sql.ErrNoRows:
			t.Fatal(err)
		case err != nil:
			t.Fatal(err)
		}
		if val != v {
			t.Fatalf("session variable value for %s is %s - expected %s", k, val, v)
		}
	}
}

func TestConnectorProxy(t *testing.T) {
	r := require.New(t)
	ctor, err := hdb.NewDSNConnector(hdb.TestDSN)
	r.NoError(err)
	ctor.SetProxy(&proxy.Config{
		Address: "127.0.0.1:1080",
	})

	db := sql.OpenDB(ctor)
	r.NoError(db.Ping())
	row := db.QueryRow("SELECT 1 AS VAL FROM DUMMY;")
	var v int64
	r.NoError(row.Scan(&v))
	r.Equal(int64(1), v)
	r.NoError(db.Close())
}

func TestConnectorProxyNoTimeout(t *testing.T) {
	r := require.New(t)
	ctor, err := hdb.NewDSNConnector(hdb.TestDSN)
	r.NoError(err)
	ctor.SetProxy(&proxy.Config{
		Address: "127.0.0.1:1080",
	})
	r.NoError(ctor.SetTimeout(0))

	db := sql.OpenDB(ctor)
	r.NoError(db.Ping())
	row := db.QueryRow("SELECT 1 AS VAL FROM DUMMY;")
	var v int64
	r.NoError(row.Scan(&v))
	r.Equal(int64(1), v)
	r.NoError(db.Close())

}
