package main

// config

const DB_FILE_DIR = "/Users/tania/DuckDB/snippets/db_files"
const FILE_COUNT = 100
const ROW_COUNT = 100
const MAX_CONCURRENCY = 8

// SQL

const CREATE_TABLE = `
	CREATE TABLE IF NOT EXISTS %s (
	  item_id 	INTEGER,
	  status_0 	INTEGER,
	  number_0 	INTEGER,
	  number_1 	INTEGER,
	  text_0 	VARCHAR,
	  text_1 	VARCHAR,
	)`
const INSERT_INTO = `
		INSERT INTO %s
		VALUES
		  (?, ?, ?, ?, ?, ?)`
const ATTACH = `ATTACH '%s'`
const SELECT_ATTACHED = `
	SELECT count(*) AS count FROM duckdb_databases()
`
