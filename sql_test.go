package asynql_test

import (
	"reflect"
	"sync"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/naoina/asynql"
)

func newTestDB(t *testing.T) *asynql.DB {
	db, err := asynql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	for _, query := range []string{
		`CREATE TABLE test_table (id INTEGER, name TEXT)`,
		`INSERT INTO test_table (id, name) VALUES (1, "alice")`,
		`INSERT INTO test_table (id, name) VALUES (2, "bob")`,
	} {
		if _, err := db.DB.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func TestDB_Exec(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()
	query := `INSERT INTO test_table (id, name) VALUES (3, "jack")`
	result := <-db.Exec(query)
	var actual interface{} = result.Err()
	var expected interface{} = nil
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf(`db.Exec(%#v); Result.Err() => %#v; want %#v`, query, actual, expected)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		t.Error(err)
	}
	actual = affected
	expected = int64(1)
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf(`db.Exec(%#v); Result.RowsAffected() => %#v; want %#v`, query, actual, expected)
	}
}

func TestDB_Query(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()
	query := `SELECT id, name FROM test_table`
	rows := <-db.Query(query)
	var actual interface{} = rows.Err()
	var expected interface{} = nil
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf(`db.Query(%#v); Rows.Err() => %#v; want %#v`, query, actual, expected)
	}
	defer rows.Close()

	var results []interface{}
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Error(err)
		}
		results = append(results, id, name)
	}
	if err := rows.Err(); err != nil {
		t.Error(err)
	}
	actual = results
	expected = []interface{}{1, "alice", 2, "bob"}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf(`db.Query(%#v) => %#v; want %#v`, query, actual, expected)
	}
}

func TestDB_QueryRow(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()
	query := `SELECT name FROM test_table WHERE id = ?`
	c1 := db.QueryRow(query, 2)
	c2 := db.QueryRow(query, 1)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		row := <-c1
		defer wg.Done()
		var name string
		if err := row.Scan(&name); err != nil {
			t.Error(err)
		}
		actual := name
		expected := "bob"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf(`db.QueryRow(%#v) => %#v; want %#v`, query, actual, expected)
		}
	}()
	go func() {
		row := <-c2
		defer wg.Done()
		var name string
		if err := row.Scan(&name); err != nil {
			t.Error(err)
		}
		actual := name
		expected := "alice"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf(`db.QueryRow(%#v) => %#v; want %#v`, query, actual, expected)
		}
	}()
	wg.Wait()
}

func TestStmt_Exec(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()
	query := `INSERT INTO test_table (id, name) VALUES (?, ?)`
	stmt, err := db.Prepare(query)
	if err != nil {
		t.Fatal(err)
	}
	result := <-stmt.Exec(3, "kay")
	var actual interface{} = result.Err()
	var expected interface{} = nil
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf(`db.Exec(%#v); Result.Err() => %#v; want %#v`, query, actual, expected)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		t.Error(err)
	}
	actual = affected
	expected = int64(1)
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf(`db.Exec(%#v); Result.RowsAffected() => %#v; want %#v`, query, actual, expected)
	}
}

func TestStmt_Query(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()
	query := `SELECT id, name FROM test_table WHERE id = ?`
	stmt, err := db.Prepare(query)
	if err != nil {
		t.Fatal(err)
	}
	rows := <-stmt.Query(2)
	var actual interface{} = rows.Err()
	var expected interface{} = nil
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf(`db.Query(%#v); Rows.Err() => %#v; want %#v`, query, actual, expected)
	}
	defer rows.Close()

	var results []interface{}
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Error(err)
		}
		results = append(results, id, name)
	}
	if err := rows.Err(); err != nil {
		t.Error(err)
	}
	actual = results
	expected = []interface{}{2, "bob"}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf(`db.Query(%#v) => %#v; want %#v`, query, actual, expected)
	}
}

func TestStmt_QueryRow(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()
	query := `SELECT name FROM test_table WHERE id = ?`
	stmt, err := db.Prepare(query)
	if err != nil {
		t.Fatal(err)
	}
	c1 := stmt.QueryRow(2)
	c2 := stmt.QueryRow(1)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		row := <-c1
		defer wg.Done()
		var name string
		if err := row.Scan(&name); err != nil {
			t.Error(err)
		}
		actual := name
		expected := "bob"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf(`db.QueryRow(%#v) => %#v; want %#v`, query, actual, expected)
		}
	}()
	go func() {
		row := <-c2
		defer wg.Done()
		var name string
		if err := row.Scan(&name); err != nil {
			t.Error(err)
		}
		actual := name
		expected := "alice"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf(`db.QueryRow(%#v) => %#v; want %#v`, query, actual, expected)
		}
	}()
	wg.Wait()
}

func TestTx_Exec(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	query := `INSERT INTO test_table (id, name) VALUES (4, "sara")`
	result := <-tx.Exec(query)
	var actual interface{} = result.Err()
	var expected interface{} = nil
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf(`db.Exec(%#v); Result.Err() => %#v; want %#v`, query, actual, expected)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		t.Error(err)
	}
	actual = affected
	expected = int64(1)
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf(`db.Exec(%#v); Result.RowsAffected() => %#v; want %#v`, query, actual, expected)
	}

	actual = tx.Commit()
	expected = nil
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf(`tx.Commit() => %#v; want %#v`, actual, expected)
	}
}

func TestTx_Query(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	query := `SELECT id, name FROM test_table ORDER BY id DESC`
	rows := <-tx.Query(query)
	var actual interface{} = rows.Err()
	var expected interface{} = nil
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf(`db.Query(%#v); Rows.Err() => %#v; want %#v`, query, actual, expected)
	}
	defer rows.Close()

	var results []interface{}
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Error(err)
		}
		results = append(results, id, name)
	}
	if err := rows.Err(); err != nil {
		t.Error(err)
	}
	actual = results
	expected = []interface{}{2, "bob", 1, "alice"}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf(`db.Query(%#v) => %#v; want %#v`, query, actual, expected)
	}

	actual = tx.Commit()
	expected = nil
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf(`tx.Commit() => %#v; want %#v`, actual, expected)
	}
}

func TestTX_QueryRow(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	query := `SELECT name FROM test_table WHERE id = ?`
	c1 := tx.QueryRow(query, 2)
	c2 := tx.QueryRow(query, 1)
	go func() {
		row := <-c1
		var name string
		if err := row.Scan(&name); err != nil {
			t.Error(err)
		}
		actual := name
		expected := "bob"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf(`db.QueryRow(%#v) => %#v; want %#v`, query, actual, expected)
		}
	}()
	go func() {
		row := <-c2
		var name string
		if err := row.Scan(&name); err != nil {
			t.Error(err)
		}
		actual := name
		expected := "alice"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf(`db.QueryRow(%#v) => %#v; want %#v`, query, actual, expected)
		}
	}()
	actual := tx.Commit()
	expected := error(nil)
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf(`tx.Commit() => %#v; want %#v`, actual, expected)
	}
}

func TestTX_Rollback(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	query := `SELECT name FROM test_table WHERE id = ?`
	c1 := tx.QueryRow(query, 2)
	c2 := tx.QueryRow(query, 1)
	go func() {
		row := <-c1
		var name string
		if err := row.Scan(&name); err != nil {
			t.Error(err)
		}
		actual := name
		expected := "bob"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf(`db.QueryRow(%#v) => %#v; want %#v`, query, actual, expected)
		}
	}()
	go func() {
		row := <-c2
		var name string
		if err := row.Scan(&name); err != nil {
			t.Error(err)
		}
		actual := name
		expected := "alice"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf(`db.QueryRow(%#v) => %#v; want %#v`, query, actual, expected)
		}
	}()
	actual := tx.Rollback()
	expected := error(nil)
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf(`tx.Rollback() => %#v; want %#v`, actual, expected)
	}
}

func TestTX_Stmt(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()
	query := `SELECT id FROM test_table WHERE name = ?`
	stmt, err := db.Prepare(query)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	stmt = tx.Stmt(stmt)
	go func() {
		name := "alice"
		rows := <-stmt.Query(name)
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err != nil {
				t.Error(err)
			}
			actual := id
			expected := 1
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf(`stmt.Query(%#v) => %#v; want %#v`, name, actual, expected)
			}
		}
		actual := rows.Err()
		expected := error(nil)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf(`stmt.Query(%#v); rows.Err() => %#v; want %#v`, name, actual, expected)
		}
	}()
	go func() {
		name := "alice"
		rows := <-stmt.Query(name)
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err != nil {
				t.Error(err)
			}
			actual := id
			expected := 1
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf(`stmt.Query(%#v) => %#v; want %#v`, name, actual, expected)
			}
		}
		actual := rows.Err()
		expected := error(nil)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf(`stmt.Query(%#v); rows.Err() => %#v; want %#v`, name, actual, expected)
		}
	}()
	var actual interface{} = tx.Commit()
	var expected interface{} = nil
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf(`tx.Commit() => %#v; want %#v`, actual, expected)
	}
}
