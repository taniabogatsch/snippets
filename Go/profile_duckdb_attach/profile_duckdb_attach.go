package main

import (
	"database/sql"
	_ "github.com/marcboeker/go-duckdb"
)

func main() {

	// configuration
	fileCount := 100000
	queryCount := 400000

	// filling the files
	var files []fileT
	fillFiles(&files, fileCount)

	// open a main duckdb instance
	db, err := sql.Open("duckdb", "")
	check("failed to open main db instance", err)
	defer func(db *sql.DB) {
		err := db.Close()
		check("failed to close main db", err)
	}(db)

	// disable some optimizers
	_, err = db.Exec(DISABLE_OPTIMIZERS)
	check("failed to disable optimizers", err)

	attachFiles(db, &files)
	queryFiles(db, queryCount, &files)
}
