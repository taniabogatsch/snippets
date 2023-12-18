package main

import (
	"log"
	"strconv"
)

type fileT struct {
	id   int
	name string
	path string
}

func fillFile(id int) (file fileT) {
	file.id = id
	part := strconv.Itoa(id % 1000)
	name := strconv.Itoa(id)

	// /Users/tania/DuckDB/files/databases/rows_100/part_16/
	file.path = FILE_DIR + FOLDER_PREFIX + part + "/"

	// board__100__000016
	for len(name) != FILE_NAME_PADDING {
		name = "0" + name
	}
	file.name = FILE_NAME_PREFIX + name
	file.path += file.name + "/"

	return file
}

func getFilePath(file *fileT, suffix string) (filepath string) {
	return file.path + file.name + suffix
}

func getTableName(file *fileT) (tableName string) {
	return file.name + TABLE_NAME
}

func fillFiles(files *[]fileT, count int) {

	log.Printf("fill %d file structs", count)
	defer measure("fill file structs")()

	for i := 0; i < count; i++ {
		*files = append(*files, fillFile(i))
	}
}
