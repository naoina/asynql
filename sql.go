// Package asynql provides channel-based asynchronous extensions on Go's database/sql.
package asynql

import (
	"database/sql"
	"sync"
)

// DB is same the sql.DB, but some methods have been provided as asynchronous implementation.
type DB struct {
	*sql.DB
}

// Open is the same as sql.Open, but returns an *asynql.DB instead.
func Open(driverName, dataSourceName string) (*DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return &DB{
		DB: db,
	}, nil
}

// Begin starts a transaction and returns an *asynql.Tx instead of an *sql.Tx.
func (db *DB) Begin() (*Tx, error) {
	tx, err := db.DB.Begin()
	if err != nil {
		return nil, err
	}
	return &Tx{
		Tx: tx,
	}, nil
}

// Exec is similar to sql.DB.Exec, but returns a channel of *asynql.Result.
// Exec executes query with args and then sends the result on the returned channel.
func (db *DB) Exec(query string, args ...interface{}) <-chan *Result {
	ch := make(chan *Result)
	go func() {
		result, err := db.DB.Exec(query, args...)
		ch <- &Result{
			Result: result,
			err:    err,
		}
	}()
	return ch
}

// Prepare is the same as sql.DB.Prepare, but returns a *asynql.Stmt instead.
func (db *DB) Prepare(query string) (*Stmt, error) {
	stmt, err := db.DB.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &Stmt{
		Stmt: stmt,
	}, nil
}

// Query is similar to sql.DB.Query, but returns a channel of *asynql.Rows.
// Query executes a query with args and then sends the result on the returned channel.
func (db *DB) Query(query string, args ...interface{}) <-chan *Rows {
	ch := make(chan *Rows)
	go func() {
		rows, err := db.DB.Query(query, args...)
		ch <- &Rows{
			Rows: rows,
			err:  err,
		}
	}()
	return ch
}

// QueryRow is similar to sql.DB.QueryRow, but returns a channel of *asynql.Row.
// QueryRow executes a query with args and then sends the result on the returned channel.
func (db *DB) QueryRow(query string, args ...interface{}) <-chan *Row {
	ch := make(chan *Row)
	go func() {
		row := db.DB.QueryRow(query, args...)
		ch <- &Row{
			Row: row,
		}
	}()
	return ch
}

// Result represents a result of Exec.
type Result struct {
	sql.Result

	err error
}

// Err returns an error.
func (r *Result) Err() error {
	return r.err
}

// Row represents a result of QueryRow.
type Row struct {
	*sql.Row
}

// Rows represents a result of a query.
type Rows struct {
	*sql.Rows

	err error
}

// Err returns an error.
func (rs *Rows) Err() error {
	if rs.err != nil {
		return rs.err
	}
	return rs.Rows.Err()
}

// Stmt is same the sql.Stmt, but some methods have been provided as asynchronous implementation.
type Stmt struct {
	*sql.Stmt

	wg *sync.WaitGroup
}

// Exec is similar to sql.Stmt.Exec, but returns a channel of *asynql.Result.
// Exec executes query with args and then sends the result on the returned channel.
func (s *Stmt) Exec(args ...interface{}) <-chan *Result {
	if s.wg != nil {
		s.wg.Add(1)
	}
	ch := make(chan *Result)
	go func() {
		result, err := s.Stmt.Exec(args...)
		ch <- &Result{
			Result: result,
			err:    err,
		}
		if s.wg != nil {
			s.wg.Done()
		}
	}()
	return ch
}

// Query is similar to sql.Stmt.Query, but returns a channel of *asynql.Rows.
// Query executes a query with args and then sends the result on the returned channel.
func (s *Stmt) Query(args ...interface{}) <-chan *Rows {
	if s.wg != nil {
		s.wg.Add(1)
	}
	ch := make(chan *Rows)
	go func() {
		rows, err := s.Stmt.Query(args...)
		ch <- &Rows{
			Rows: rows,
			err:  err,
		}
		if s.wg != nil {
			s.wg.Done()
		}
	}()
	return ch
}

// QueryRow is similar to sql.Stmt.QueryRow, but returns a channel of *asynql.Row.
// QueryRow executes a query with args and then sends the result on the returned channel.
func (s *Stmt) QueryRow(args ...interface{}) <-chan *Row {
	if s.wg != nil {
		s.wg.Add(1)
	}
	ch := make(chan *Row)
	go func() {
		row := s.Stmt.QueryRow(args...)
		ch <- &Row{
			Row: row,
		}
		if s.wg != nil {
			s.wg.Done()
		}
	}()
	return ch
}

// Tx is same the sql.Tx, but some methods have been provided as asynchronous implementation.
type Tx struct {
	*sql.Tx

	wg sync.WaitGroup
}

// Commit is same the sql.Tx.Commit, but waits the end of the all queries.
func (tx *Tx) Commit() error {
	tx.wg.Wait()
	return tx.Tx.Commit()
}

// Exec is similar to sql.Tx.Exec, but returns a channel of *asynql.Result.
// Exec executes query with args and then sends the result on the returned channel.
func (tx *Tx) Exec(query string, args ...interface{}) <-chan *Result {
	tx.wg.Add(1)
	ch := make(chan *Result)
	go func() {
		result, err := tx.Tx.Exec(query, args...)
		ch <- &Result{
			Result: result,
			err:    err,
		}
		tx.wg.Done()
	}()
	return ch
}

// Prepare is the same as sql.Tx.Prepare, but returns a *asynql.Stmt instead.
func (tx *Tx) Prepare(query string) (*Stmt, error) {
	stmt, err := tx.Tx.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &Stmt{
		Stmt: stmt,
		wg:   &tx.wg,
	}, nil
}

// Query is similar to sql.Tx.Query, but returns a channel of *asynql.Rows.
// Query executes a query with args and then sends the result on the returned channel.
func (tx *Tx) Query(query string, args ...interface{}) <-chan *Rows {
	tx.wg.Add(1)
	ch := make(chan *Rows)
	go func() {
		rows, err := tx.Tx.Query(query, args...)
		ch <- &Rows{
			Rows: rows,
			err:  err,
		}
		tx.wg.Done()
	}()
	return ch
}

// QueryRow is similar to sql.Tx.QueryRow, but returns a channel of *asynql.Row.
// QueryRow executes a query with args and then sends the result on the returned channel.
func (tx *Tx) QueryRow(query string, args ...interface{}) <-chan *Row {
	tx.wg.Add(1)
	ch := make(chan *Row)
	go func() {
		row := tx.Tx.QueryRow(query, args...)
		ch <- &Row{
			Row: row,
		}
		tx.wg.Done()
	}()
	return ch
}

// Rollback is same the sql.Tx.Rollback, but waits the end of the all queries.
func (tx *Tx) Rollback() error {
	tx.wg.Wait()
	return tx.Tx.Rollback()
}

// Stmt is same the sql.Tx.Stmt, but returns a *asynql.Stmt.
func (tx *Tx) Stmt(stmt *Stmt) *Stmt {
	return &Stmt{
		Stmt: tx.Tx.Stmt(stmt.Stmt),
		wg:   &tx.wg,
	}
}
