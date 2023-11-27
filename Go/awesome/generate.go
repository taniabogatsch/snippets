package main

import (
	"database/sql"
	"fmt"
	"github.com/go-faker/faker/v4"
	"log"
	"math/rand"
	"os"
	"sync"
)

func generateFiles() {

	log.Printf("generating %d database files", FILE_COUNT)
	defer measure("file generation")()

	filepath := fmt.Sprintf("%s/%d_files", DB_FILE_DIR, FILE_COUNT)
	log.Printf("filepath: %s", filepath)

	sem := make(chan int, MAX_CONCURRENCY)
	var waitGroup sync.WaitGroup
	waitGroup.Add(FILE_COUNT)

	for i := 0; i < FILE_COUNT; i++ {
		// will block if there is MAX_CONCURRENCY integers in sem
		sem <- 1
		tableName := fmt.Sprintf("board_%d", i)
		go createTable(tableName, filepath, &waitGroup, sem)

		if i%1000 == 0 {
			fmt.Print(".")
		}
	}

	waitGroup.Wait()
}

func createTable(tableName string, filepath string, waitGroup *sync.WaitGroup, sem chan int) {
	defer waitGroup.Done()

	err := os.MkdirAll(filepath, os.ModePerm)
	check("failed to create directory", err)
	filename := fmt.Sprintf("%s.db", tableName)

	db, err := sql.Open("duckdb", fmt.Sprintf("%s/%s", filepath, filename))
	check("failed to open db", err)
	defer func(db *sql.DB) {
		err := db.Close()
		check("failed to close db", err)
	}(db)

	_, err = db.Exec(fmt.Sprintf(CREATE_TABLE, tableName))
	check("failed to create table", err)

	// insert ROW_COUNT random rows
	// FIXME: use appender for bulk insertions
	for i := 0; i < ROW_COUNT; i++ {
		_, err = db.Exec(fmt.Sprintf(INSERT_INTO, tableName),
			i, rand.Intn(10), rand.Intn(1000), rand.Intn(1000), faker.Sentence(), faker.Sentence())
		check("failed to insert row", err)
	}

	// removes an int from sem, allowing another to proceed
	<-sem
}
