#include <iostream>
#include <string>
#include <unistd.h>
#include <sys/stat.h>
#include <thread>
#include <chrono>
#include <fcntl.h>

using namespace std;

static constexpr size_t READ_COUNT = 6;
static constexpr size_t READ_SIZE = 10000;
static constexpr size_t FILE_COUNT = 50000;
static constexpr size_t RUNS = 5;

const string DB_DIR = "my_path/" + to_string(FILE_COUNT);

int file_descriptors[FILE_COUNT];
bool KEEP_OPEN = false;
bool LOCK_FILES = true;
size_t THREAD_COUNT = 0;

struct File {
    explicit File(const size_t file_id) {

        auto folder = file_id % 1000;
        auto file_id_str = to_string(file_id);

        auto max_padding = to_string(FILE_COUNT).length();
        auto padding = max_padding - file_id_str.length();

        for (size_t i = 0; i < padding; i++) {
            file_id_str = "0" + file_id_str;
        }

        filename = "board__10000__" + file_id_str;
        filepath = DB_DIR + "/" + to_string(folder) + "/" + filename + ".db";
    }

    string filename;
    string filepath;
};

static void CheckError(const bool has_error, const string &msg) {
    if (has_error) {
        cout << msg << "\n";
        exit(0);
    }
}

static void FileWorker(const size_t start, const size_t end) {

    for (size_t file_id = start; file_id < end; file_id++) {

        // get file path
        File file(file_id);
        CheckError(access(file.filepath.c_str(), 0), "failed to access file: " + file.filepath);

        // test if the file is a regular file
        struct stat status{};
        stat(file.filepath.c_str(), &status);
        CheckError(!S_ISREG(status.st_mode), "not a regular file");

        // open file
        file_descriptors[file_id] = open(file.filepath.c_str(), O_RDWR, 0666);
        CheckError(file_descriptors[file_id] == -1, "error opening file: " + string(strerror(errno)));

        // lock file
        if (LOCK_FILES) {
            int rc = flock(file_descriptors[file_id], LOCK_SH);
            CheckError(rc == -1, "could not set lock on file: " + string(strerror(errno)));
        }

        // read file
        char b[READ_COUNT * READ_SIZE];
        size_t read_bytes = 0;

        for (size_t i = 0; i < READ_COUNT; i++) {
            auto bytes = pread(file_descriptors[file_id], b, READ_SIZE, i * READ_SIZE);
            CheckError(bytes == -1, "could not read from file: " + string(strerror(errno)));
            read_bytes += bytes;
        }
        CheckError(read_bytes != READ_SIZE * READ_COUNT, "could not read all bytes: " + to_string(read_bytes));

        // close file
        if (!KEEP_OPEN) {
            CheckError(close(file_descriptors[file_id]) == -1, "error closing file");
        }
    }
}

void ReadFiles() {

    // ensure that the directory is valid
    struct stat status{};
    CheckError(stat(DB_DIR.c_str(), &status), "not a valid directory");

    // loop setup
    vector<thread> threads;
    auto files_per_thread = size_t(double(FILE_COUNT) / double(THREAD_COUNT));
    size_t remaining_files = FILE_COUNT % THREAD_COUNT;
    size_t end = 0;

    // spawn and run workers
    for (size_t i = 0; i < THREAD_COUNT; i++) {

        size_t thread_file_count = files_per_thread;
        if (i < remaining_files) {
            thread_file_count++;
        }
        size_t start = end;
        end += thread_file_count;

        threads.emplace_back(FileWorker, start, end);
    }

    for (size_t i = 0; i < THREAD_COUNT; i++) {
        threads[i].join();
    }

    if (KEEP_OPEN) {
        for (auto &file_descriptor : file_descriptors) {
            CheckError(close(file_descriptor) == -1, "error closing file");
        }
    }
}

size_t GetMedian(vector<size_t> &timings) {
    sort(timings.begin(), timings.end());
    return timings[timings.size() / 2];
}

// main

int main() {

    cout << "start reading files \n";
    cout << "runs: " << RUNS << "\n";
    cout << "keep open: " << KEEP_OPEN << "\n";
    cout << "lock files: " << LOCK_FILES << "\n\n";

    vector<size_t> thread_counts = {1, 2, 4, 8};
    for (const auto &thread_count : thread_counts) {

        THREAD_COUNT = thread_count;

        // cold run
        ReadFiles();

        vector<size_t> timings;
        for (size_t i = 0; i < RUNS; i++) {

            auto t1 = chrono::high_resolution_clock::now();
            ReadFiles();
            auto t2 = chrono::high_resolution_clock::now();

            auto ms_int = chrono::duration_cast<chrono::milliseconds>(t2 - t1);
            timings.push_back(ms_int.count());
        }

        cout << "threads: " << thread_count << " [ms]: " << GetMedian(timings) << "\n";
    }

    cout << "\n" << "finish reading files \n";
    return 0;
}
