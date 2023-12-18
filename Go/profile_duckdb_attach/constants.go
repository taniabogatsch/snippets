package main

const FILE_DIR = "/Users/tania/DuckDB/files/databases/rows_100"
const FOLDER_PREFIX = "/part_"
const FILE_NAME_PADDING = 6
const FILE_NAME_PREFIX = "board__100__"
const FILE_NAME_SUFFIX_DB = ".db"
const FILE_NAME_SUFFIX_JSON = ".json"
const TABLE_NAME = ".board_data"

const MAX_CONCURRENCY = 8

const ATTACH = `ATTACH '%s' (TYPE DUCKDB, READONLY)`
const QUERY = `SELECT * from %s WHERE name_0 like '%%2%%' ORDER BY vc_virtual_pos_vc LIMIT 100`
const SELECT_ATTACHED = `
	SELECT count(*) AS count FROM duckdb_databases()
`
const DISABLE_OPTIMIZERS = `SET disabled_optimizers TO 'JOIN_ORDER,STATISTICS_PROPAGATION'`
