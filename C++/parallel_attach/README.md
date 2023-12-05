# Profile file operations

- C++ native
- `fcntl.h` to open, lock, read, and close files
- `access` and regular file check

### Configurations

- `READ_COUNT`: how often to read a file
- `READ_SIZE`: how many bytes to read per `READ_COUNT`. File sizes must exceed `READ_COUNT * READ_SIZE`.
- `FILE_COUNT`: how many files to read
- `THREAD_COUNT`: number of concurrent file reader threads
- `DB_DIR`: directory containing greater than `FILE_COUNT` files with the following naming scheme: `board_{file_id}.db`
- `KEEP_OPEN`: if true, files are kept open until exiting the program
- `LOCK_FILES`: if true, files are locked after opening them

## Preliminary results

### Setup and configuration

- Apple M1 Max, 32GB RAM
- 10 cores, 8 performance cores

| Read count | Read size | File count |
| ----------- | ----------- | ----------- |
| 6 | 10_000 | 50_000 |
