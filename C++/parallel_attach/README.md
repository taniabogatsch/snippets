### Profile file operations

#### Characteristics

- C++ native
- `fopen`, `fread`, `fclose` instead of `fstream`

#### Configurations

- `READ_COUNT`: how often to read a file
- `READ_SIZE`: how many bytes to read per `READ_COUNT`. File sizes must exceed `READ_COUNT * READ_SIZE`.
- `FILE_COUNT`: how many files to read
- `THREAD_COUNT`: number of concurrent file reader threads
- `DB_DIR`: directory containing greater than `FILE_COUNT` files with the following naming scheme: `board_{file_id}.db`
- `KEEP_OPEN`: if true, files are kept open until exiting the program

### Preliminary results

#### Not keeping files open

```
static constexpr size_t READ_COUNT = 4;
static constexpr size_t READ_SIZE = 4096;
static constexpr size_t FILE_COUNT = 30000;
static constexpr bool KEEP_OPEN = false;
```

```
1 threads: elapsed time in [ms]: 3858
2 threads: elapsed time in [ms]: 1939
4 threads: elapsed time in [ms]: 916
8 threads: elapsed time in [ms]: 494
```

#### Keeping files open

```
static constexpr size_t READ_COUNT = 4;
static constexpr size_t READ_SIZE = 4096;
static constexpr size_t FILE_COUNT = 30000;
static constexpr bool KEEP_OPEN = true;
```

```
1 threads: elapsed time in [ms]: 5810
2 threads: elapsed time in [ms]: 2968
4 threads: elapsed time in [ms]: 1893
8 threads: elapsed time in [ms]: 1300
```