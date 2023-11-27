package main

import (
	_ "github.com/marcboeker/go-duckdb"
)

func main() {

	generateFiles()
	attachFiles()
	removeFiles()
}
