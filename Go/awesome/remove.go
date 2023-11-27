package main

import (
	"log"
	"os"
)

func removeFiles() {

	log.Print("removing files")
	err := os.RemoveAll(DB_FILE_DIR)
	check("failed to remove files", err)
}
