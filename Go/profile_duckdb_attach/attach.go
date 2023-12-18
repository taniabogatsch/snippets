package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
)

func attachFiles(db *sql.DB, files *[]fileT) {

	log.Printf("attaching %d database files", len(*files))
	defer measure("attaching databases")()

	// channel to distribute attach jobs among connections
	attachJobs := make(chan fileT, len(*files))
	var waitGroup sync.WaitGroup
	waitGroup.Add(len(*files))

	// start MAX_CONCURRENCY connections
	for i := 0; i < MAX_CONCURRENCY; i++ {
		go attachWorker(&waitGroup, attachJobs, db)
	}

	// start all file attach jobs
	for _, file := range *files {
		attachJobs <- file
	}
	close(attachJobs)

	// finish attaching databases
	waitGroup.Wait()
	verifyAttach(db, len(*files))
}

func attachWorker(waitGroup *sync.WaitGroup, attachJobs chan fileT, db *sql.DB) {

	// create the db connection of the worker
	conn, err := db.Conn(context.Background())
	check("failed to get connection", err)

	attachCount := 0

	// start working on incoming jobs
	for file := range attachJobs {

		// attach a database file
		filepath := getFilePath(&file, FILE_NAME_SUFFIX_DB)
		_, err = conn.ExecContext(context.Background(), fmt.Sprintf(ATTACH, filepath))
		check("failed to attach database", err)

		attachCount++
		if attachCount%1000 == 0 {
			fmt.Print(".")
		}
		waitGroup.Done()
	}

	// close the connection
	err = conn.Close()
	check("failed to close connection", err)
}

func verifyAttach(db *sql.DB, fileCount int) {

	// get the count of all attached database files
	var (
		count int
	)
	row := db.QueryRow(SELECT_ATTACHED)
	err := row.Scan(&count)
	check("failed to query rows", err)

	// verify that we've attached FILE_COUNT files
	if count < fileCount {
		log.Println(fmt.Sprintf("attach failed, only attached %d out of %d database files", count, fileCount))
		log.Fatal(err)
	}
}
