package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
)

func attachFiles() {

	log.Printf("attaching %d database files", FILE_COUNT)

	// same process, so multiple READ/WRITE connections are possible

	// open a main duckdb instance
	db, err := sql.Open("duckdb", "")
	check("failed to open main db instance", err)
	defer func(db *sql.DB) {
		err := db.Close()
		check("failed to close main db", err)
	}(db)

	filepath := fmt.Sprintf("%s/%d_files/", DB_FILE_DIR, FILE_COUNT)

	// get MAX_CONCURRENCY concurrent connections
	db.SetMaxOpenConns(MAX_CONCURRENCY)

	// channel to distribute attach jobs among connections
	attachJobs := make(chan int, FILE_COUNT)

	// MAX_CONCURRENCY is our maximum concurrency, as we want to have one dedicated
	// thread per connection, these threads/connections receive attach jobs from the
	// attach_jobs channel
	var waitGroup sync.WaitGroup
	waitGroup.Add(FILE_COUNT)

	defer measure("attaching databases")()

	// start CONN_POOL_SIZE connections
	for i := 0; i < MAX_CONCURRENCY; i++ {
		go attachWorker(&waitGroup, attachJobs, db, filepath)
	}

	// start FILE_COUNT attach jobs
	for j := 0; j < FILE_COUNT; j++ {
		attachJobs <- j
	}
	close(attachJobs)

	// finish attaching databases
	waitGroup.Wait()
	verifyAttach(db)
}

func attachWorker(waitGroup *sync.WaitGroup, attachJobs chan int, db *sql.DB, dirPath string) {

	// create the db connection of the worker
	conn, err := db.Conn(context.Background())
	check("failed to get connection", err)

	// start working on incoming jobs
	for job := range attachJobs {

		// attach a database file
		filepath := fmt.Sprintf("%sboard_%d.db", dirPath, job)
		_, err = conn.ExecContext(context.Background(), fmt.Sprintf(ATTACH, filepath))
		check("failed to attach database", err)

		if job%10000 == 0 {
			fmt.Print(".")
		}
		waitGroup.Done()
	}

	// close the connection
	err = conn.Close()
	check("failed to close connection", err)
}

func verifyAttach(db *sql.DB) {

	// get the count of all attached database files
	var (
		count int
	)
	row := db.QueryRow(SELECT_ATTACHED)
	err := row.Scan(&count)
	check("failed to query rows", err)

	// verify that we've attached FILE_COUNT files
	if count < FILE_COUNT {
		log.Println(fmt.Sprintf("attach failed, only attached %d out of %d database files", count, FILE_COUNT))
		log.Fatal(err)
	}
}
