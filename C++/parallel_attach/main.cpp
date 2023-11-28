#include <iostream>
#include <string>
#include <cstdio>
#include <unistd.h>
#include <sys/stat.h>
#include <thread>
#include <chrono>

using namespace std;

// constants

static constexpr size_t READ_COUNT = 4;
static constexpr size_t READ_SIZE = 4096;
static constexpr size_t FILE_COUNT = 30000;
static constexpr size_t THREAD_COUNT = 8;

const string DB_DIR = "my_path";

FILE * file_pointers[FILE_COUNT];
static constexpr bool KEEP_OPEN = false;

// functions

static void CheckError(const bool has_error, const string &msg) {
    if (has_error) {
        cout << msg << "\n";
        exit(0);
    }
}

static void FileWorker(const size_t start, const size_t end) {

    for (size_t file_id = start; file_id < end; file_id++) {

        // get the file path
        auto file_path = DB_DIR + "/board_" + to_string(file_id) + ".db";
        CheckError(access(file_path.c_str(), 0), "failed to access file: " + file_path);

        // test if the file is a regular file
        struct stat status{};
        stat(file_path.c_str(), &status);
        CheckError(!S_ISREG(status.st_mode), "not a regular file");

        // open the file
        file_pointers[file_id] = fopen(file_path.c_str(), "rb");
        CheckError(!file_pointers[file_id], "error opening file");

        // read the file
        char b[READ_COUNT * READ_SIZE];
        size_t read_bytes = 0;

        for (size_t i = 0; i < READ_COUNT; i++) {
            read_bytes += fread(b, sizeof(char), READ_SIZE, file_pointers[file_id]);
        }

        if (read_bytes != READ_SIZE * READ_COUNT) {
            CheckError(feof(file_pointers[file_id]), "unexpected end of file");
            CheckError(ferror(file_pointers[file_id]), "error reading file");
        }

        // close file
        if (!KEEP_OPEN) {
            CheckError(fclose(file_pointers[file_id]), "error closing file");
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
        for (auto &file_pointer : file_pointers) {
            CheckError(fclose(file_pointer), "error closing file");
        }
    }
}

// main

int main() {
    cout << "start reading files \n";
    auto t1 = chrono::high_resolution_clock::now();

    ReadFiles();

    auto t2 = chrono::high_resolution_clock::now();
    cout << "finish reading files \n";

    auto ms_int = chrono::duration_cast<chrono::milliseconds>(t2 - t1);
    auto s_int = chrono::duration_cast<chrono::seconds>(t2 - t1);
    cout << "elapsed time in [ms]: " << ms_int.count() << "\n";
    cout << "elapsed time in [s]: " << s_int.count() << "\n";

    return 0;
}
