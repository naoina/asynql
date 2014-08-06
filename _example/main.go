package main

import (
	"fmt"
	"log"

	_ "github.com/mattn/go-sqlite3"
	"github.com/naoina/asynql"
)

func CreateTable(db *asynql.DB) error {
	result := db.Exec(`CREATE TABLE IF NOT EXISTS test_table (id INTEGER, name TEXT)`)
	r := <-result
	return r.Err()
}

func Insert(db *asynql.DB) error {
	r1 := db.Exec(`INSERT INTO test_table (id, name) VALUES (?, ?)`, 1, "alice")
	r2 := db.Exec(`INSERT INTO test_table (id, name) VALUES (?, ?)`, 2, "bob")
	r3 := db.Exec(`INSERT INTO test_table (id, name) VALUES (?, ?)`, 3, "jack")
	for _, ch := range []<-chan *asynql.Result{r1, r2, r3} {
		result := <-ch
		if err := result.Err(); err != nil {
			return err
		}
	}
	return nil
}

func Query(db *asynql.DB) error {
	rows1 := db.Query(`SELECT id, name FROM test_table`)
	row2 := db.QueryRow(`SELECT COUNT(*) FROM test_table`)
	r1 := <-rows1
	if err := r1.Err(); err != nil {
		return err
	}
	defer r1.Close()
	for r1.Next() {
		var id int
		var name string
		if err := r1.Scan(&id, &name); err != nil {
			return err
		}
		fmt.Printf("id: %v, name: %v\n", id, name)
	}
	if err := r1.Err(); err != nil {
		return err
	}
	var count int
	r2 := <-row2
	if err := r2.Scan(&count); err != nil {
		return err
	}
	fmt.Printf("count: %v\n", count)
	return nil
}

func main() {
	db, err := asynql.Open("sqlite3", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if err := CreateTable(db); err != nil {
		log.Fatal(err)
	}
	if err := Insert(db); err != nil {
		log.Fatal(err)
	}
	if err := Query(db); err != nil {
		log.Fatal(err)
	}
}
