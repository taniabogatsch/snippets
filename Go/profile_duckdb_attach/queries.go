package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"sync"
)

func queryFiles(db *sql.DB, queryCount int, files *[]fileT) {

	log.Printf("querying %d database files", queryCount)
	defer measure("querying databases")()

	queryJobs := make(chan fileT, queryCount)

	var waitGroup sync.WaitGroup
	waitGroup.Add(queryCount)

	for i := 0; i < MAX_CONCURRENCY; i++ {
		go queryWorker(&waitGroup, queryJobs, db)
	}

	for i := 0; i < queryCount; i++ {
		fileId := rand.Intn(len(*files))
		queryJobs <- (*files)[fileId]
	}

	close(queryJobs)
	waitGroup.Wait()
}

func queryWorker(waitGroup *sync.WaitGroup, queryJobs chan fileT, db *sql.DB) {

	// create the db connection of the worker
	conn, err := db.Conn(context.Background())
	check("failed to get connection", err)

	queryCount := 0

	// start working on incoming jobs
	for file := range queryJobs {

		// query a database file
		_, err = conn.ExecContext(context.Background(), fmt.Sprintf(QUERY, getTableName(&file)))
		check("failed to attach database", err)

		queryCount++
		if queryCount%1000 == 0 {
			fmt.Print(".")
		}
		waitGroup.Done()
	}

	// close the connection
	err = conn.Close()
	check("failed to close connection", err)
}
