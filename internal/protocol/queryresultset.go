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

package protocol

import (
	"database/sql/driver"
	"io"
	"reflect"
	"sync"
)

/*
Definition of queryResultSet in protocol layer:
- queryResultSet is sql.Rows
- sql.Rows can be used as datatype for scan
- used ig go-hdb for call table output parameters
*/

// NoResult is the driver.Rows drop-in replacement if driver Query or QueryRow is used for statements that do not return rows.
var noResult = new(noResultType)

//  check if noResultType implements all required interfaces
var (
	_ driver.Rows = (*noResultType)(nil)
)

var noColumns = []string{}

type noResultType struct{}

func (r *noResultType) Columns() []string              { return noColumns }
func (r *noResultType) Close() error                   { return nil }
func (r *noResultType) Next(dest []driver.Value) error { return io.EOF }

// query result set

//  check if queryResult implements all required interfaces
var (
	_ driver.Rows                           = (*queryResultSet)(nil)
	_ driver.RowsColumnTypeDatabaseTypeName = (*queryResultSet)(nil) // go 1.8
	_ driver.RowsColumnTypeLength           = (*queryResultSet)(nil) // go 1.8
	_ driver.RowsColumnTypeNullable         = (*queryResultSet)(nil) // go 1.8
	_ driver.RowsColumnTypePrecisionScale   = (*queryResultSet)(nil) // go 1.8
	_ driver.RowsColumnTypeScanType         = (*queryResultSet)(nil) // go 1.8
	_ driver.RowsNextResultSet              = (*queryResultSet)(nil) // go 1.8
)

type queryResultSet struct {
	s       *Session
	rrs     []rowsResult
	rr      rowsResult
	idx     int // current result set
	pos     int
	lastErr error
}

func newQueryResultSet(s *Session, rrs ...rowsResult) *queryResultSet {
	if len(rrs) == 0 {
		panic("query result set is empty")
	}
	return &queryResultSet{s: s, rrs: rrs, rr: rrs[0]}
}

func (r *queryResultSet) Columns() []string {
	return r.rr.columns()
}

func (r *queryResultSet) Close() error {
	// if lastError is set, attrs are nil
	if r.lastErr != nil {
		return r.lastErr
	}

	if !r.rr.closed() {
		return r.s.CloseResultsetID(r.rr.rsID())
	}
	return nil
}

func (r *queryResultSet) Next(dest []driver.Value) error {
	if r.s.IsBad() {
		return driver.ErrBadConn
	}

	if r.pos >= r.rr.numRow() {
		if r.rr.lastPacket() {
			return io.EOF
		}
		if err := r.s.fetchNext(r.rr); err != nil {
			r.lastErr = err //fieldValues and attrs are nil
			return err
		}
		if r.rr.numRow() == 0 {
			return io.EOF
		}
		r.pos = 0
	}

	r.rr.copyRow(r.pos, dest)
	r.pos++

	// TODO eliminate
	for _, v := range dest {
		if v, ok := v.(sessionSetter); ok {
			v.setSession(r.s)
		}
	}
	return nil
}

func (r *queryResultSet) HasNextResultSet() bool {
	return (r.idx + 1) < len(r.rrs)
}

func (r *queryResultSet) NextResultSet() error {
	if !r.HasNextResultSet() {
		return io.EOF
	}
	r.lastErr = nil
	r.idx++
	r.rr = r.rrs[r.idx]
	return nil
}

func (r *queryResultSet) ColumnTypeDatabaseTypeName(idx int) string {
	return r.rr.field(idx).TypeName()
}

func (r *queryResultSet) ColumnTypeLength(idx int) (int64, bool) {
	return r.rr.field(idx).TypeLength()
}

func (r *queryResultSet) ColumnTypePrecisionScale(idx int) (int64, int64, bool) {
	return r.rr.field(idx).TypePrecisionScale()
}

func (r *queryResultSet) ColumnTypeNullable(idx int) (bool, bool) {
	return r.rr.field(idx).Nullable(), true
}

func (r *queryResultSet) ColumnTypeScanType(idx int) reflect.Type {
	return scanTypeMap[r.rr.field(idx).ScanType()]
}

// QrsCache is a query result cache supporting reading
// procedure (call) table parameter via separate query (legacy mode).
var QrsCache = newQueryResultSetCache()

type queryResultSetCache struct {
	cache map[uint64]*queryResultSet
	mu    sync.RWMutex
}

func newQueryResultSetCache() *queryResultSetCache {
	return &queryResultSetCache{
		cache: map[uint64]*queryResultSet{},
	}
}

func (c *queryResultSetCache) set(id uint64, qrs *queryResultSet) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache[id] = qrs
	return id
}

func (c *queryResultSetCache) Get(id uint64) (*queryResultSet, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	qrs, ok := c.cache[id]
	return qrs, ok
}

func (c *queryResultSetCache) cleanup(s *Session) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, qrs := range c.cache {
		if qrs.s == s {
			delete(c.cache, id)
		}
	}
}
