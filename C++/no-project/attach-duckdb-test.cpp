#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/common/random_engine.hpp"

#include <iostream>
#include <thread>
#include <list>

using namespace duckdb;
using namespace std;

const string DB_DIR_BASE = "/Users/tania/DuckDB/files/databases";

struct Config {
	bool use_lru;

	idx_t query_count;
	idx_t file_count;
	idx_t worker_count;
	idx_t lru_size = 0;

	// how many % of queries to chose from the small range
	idx_t skew;
	// skew amount of queries go to file_count / ratio
	idx_t ratio;

	idx_t total_attached_files = 0;
	idx_t avg_attached_files = 0;
	idx_t max_attached_files = 0;

	idx_t ms = 0;
	idx_t s = 0;
	idx_t failed_queries = 0;

	string DbDir() {
		return DB_DIR_BASE + "/" + to_string(file_count);
	}

	void PrintCSV() const {
		auto time_per_query = double(ms) / double(query_count);

		cout << use_lru << "," << query_count << "," << file_count << "," << worker_count << "," << lru_size << ",";
		cout << skew << "," << ratio << ",";
		cout << total_attached_files << "," << avg_attached_files << "," << max_attached_files << ",";
		cout << ms << "," << s << "," << time_per_query << "," << failed_queries;
		cout << "\n";
	}

	void Print() const {

		cout << "\n";
		cout << "use LRU: " << use_lru << "\n";

		cout << "query count: " << query_count << "\n";
		cout << "file count: " << file_count << "\n";
		cout << "worker count: " << worker_count << "\n";
		cout << "LRU size: " << lru_size << "\n";

		cout << "skew: " << skew << "\n";
		cout << "ratio: " << ratio << "\n";

		cout << "total attached files: " << total_attached_files << "\n";
		cout << "avg attach per file: " << avg_attached_files << "\n";
		cout << "max attach per file: " << max_attached_files << "\n";

		cout << "elapsed time in [ms]: " << ms << "\n";
		cout << "elapsed time in [s]: " << s << "\n";
		cout << "elapsed time per query in [ms]: " << double(ms) / double(query_count) << "\n";
		cout << "failed queries: " << failed_queries << "\n";
	}
};

Config CURRENT_CONFIG;

struct File {
	explicit File(const idx_t file_id) {

		auto folder = file_id % 1000;
		auto file_id_str = to_string(file_id);

		auto max_padding = to_string(CURRENT_CONFIG.file_count).length();
		auto padding = max_padding - file_id_str.length();

		for (idx_t i = 0; i < padding; i++) {
			file_id_str = "0" + file_id_str;
		}

		filename = "board__1000__" + file_id_str;
		filepath = CURRENT_CONFIG.DbDir() + "/" + to_string(folder) + "/" + filename + ".db";
	}

	idx_t usage_count = 0;
	string filename;
	string filepath;
};

struct Files {
	mutex lock;
	unordered_map<idx_t, File> files;

	File Insert(const idx_t file_id) {
		{
			lock_guard<mutex> file_lock(lock);
			files.insert(make_pair(file_id, File(file_id)));
		}
		return Get(file_id, false);
	}

	File Get(const idx_t file_id, const bool increase_usage) {
		lock_guard<mutex> file_lock(lock);
		auto &file = files.find(file_id)->second;
		if (increase_usage) {
			file.usage_count++;
		}
		return file;
	}
};

// taken from https://stackoverflow.com/a/14503492
template <class KEY_T>
class LRUCache {
public:
	explicit LRUCache(idx_t lru_size, DuckDB &db) : lru_size(lru_size), db(db) {
	}
	//! Adds a new entry to the LRU
	void PutKey(const KEY_T &key) {
		auto it = item_map.find(key);
		if (it != item_map.end()) {
			item_list.erase(it->second);
			item_map.erase(it);
		}
		item_list.push_front(key);
		item_map.insert(make_pair(key, item_list.begin()));
		Evict();
	}
	//! Moves the key to the front of the LRU, if it exists
	bool KeyExists(const KEY_T &key) {
		auto it = item_map.find(key);
		if (it == item_map.end()) {
			return false;
		}
		item_list.splice(item_list.begin(), item_list, it->second);
		return true;
	}

private:
	list<KEY_T> item_list;
	unordered_map<KEY_T, decltype(item_list.begin())> item_map;
	idx_t lru_size;
	DuckDB &db;

private:
	void Evict() {
		while (item_map.size() > lru_size) {
			auto end = item_list.end();
			end--;

			// DETACH
			File file(*end);
			Connection con(db);
			REQUIRE_NO_FAIL(con.Query("DETACH " + file.filename));

			item_map.erase(*end);
			item_list.pop_back();
		}
	}
};

idx_t GetFileId(RandomEngine &engine) {

	auto rand = engine.NextRandomInteger();

	if (rand % 1000 < CURRENT_CONFIG.skew) {
		auto ratio_files = CURRENT_CONFIG.file_count / CURRENT_CONFIG.ratio;
		return rand % ratio_files;
	}

	return rand % CURRENT_CONFIG.file_count;
}

struct AdHocManager {

	mutex lock;
	idx_t total_attached_files = 0;
	unordered_map<string, idx_t> usage_counts;

	void Attach(const idx_t, const File &file, Connection &con) {

		lock_guard<mutex> attach_lock(lock);

		auto it = usage_counts.find(file.filepath);
		if (it != usage_counts.end()) {
			it->second++;
			return;
		}

		usage_counts.insert(make_pair(file.filepath, 1));
		REQUIRE_NO_FAIL(con.Query("ATTACH IF NOT EXISTS'" + file.filepath + "' (TYPE DUCKDB);"));
		total_attached_files++;
	}

	void Detach(const File &file, Connection &con) {

		lock_guard<mutex> detach_lock(lock);

		auto it = usage_counts.find(file.filepath);
		REQUIRE(it != usage_counts.end());

		it->second--;
		if (!it->second) {
			REQUIRE_NO_FAIL(con.Query("DETACH " + file.filename));
			usage_counts.erase(it);
		}
	}
};

struct LRUManager {

	explicit LRUManager(const idx_t lru_size, DuckDB &db) : lru(LRUCache<idx_t>(lru_size, db)) {
	}

	mutex lock;
	idx_t total_attached_files = 0;
	LRUCache<idx_t> lru;

	void Attach(const idx_t file_id, const File &file, Connection &con) {

		lock_guard<mutex> attach_lock(lock);

		if (lru.KeyExists(file_id)) {
			return;
		}

		lru.PutKey(file_id);
		REQUIRE_NO_FAIL(con.Query("ATTACH IF NOT EXISTS'" + file.filepath + "' (TYPE DUCKDB);"));
		total_attached_files++;
	}

	void Detach(const File &, Connection &) {
		// NOP
	}
};

struct AttachManager {

	explicit AttachManager(DuckDB &db, const idx_t lru_size) : lru_manager(lru_size, db) {
	}

	AdHocManager ad_hoc_manager;
	LRUManager lru_manager;
	Files files;

	void Attach(const idx_t file_id, Connection &con) {
		auto file = files.Get(file_id, true);
		if (CURRENT_CONFIG.use_lru) {
			return lru_manager.Attach(file_id, file, con);
		}
		ad_hoc_manager.Attach(file_id, file, con);
	}

	void Detach(const idx_t file_id, Connection &con) {
		auto file = files.Get(file_id, false);
		if (CURRENT_CONFIG.use_lru) {
			return lru_manager.Detach(file, con);
		}
		ad_hoc_manager.Detach(file, con);
	}

	idx_t TotalAttachedFiles() {
		if (CURRENT_CONFIG.use_lru) {
			return lru_manager.total_attached_files;
		}
		return ad_hoc_manager.total_attached_files;
	}
	idx_t AvgAttachPerFile() {
		idx_t sum = 0;
		for (auto it = files.files.begin(); it != files.files.end(); it++) {
			sum += it->second.usage_count;
		}
		return sum / files.files.size();
	}
	idx_t MaxAttachPerFile() {
		idx_t max = 0;
		for (auto it = files.files.begin(); it != files.files.end(); it++) {
			if (it->second.usage_count > max) {
				max = it->second.usage_count;
			}
		}
		return max;
	}
};

static void Query(AttachManager &manager, Connection &con, const idx_t file_id) {

	auto file = manager.files.Insert(file_id);

	manager.Attach(file_id, con);
	auto result = con.Query("SELECT * from " + file.filename +
	                        ".board_data WHERE status_0 = 5 AND text_0 = 'text' AND number_0 = 500 "
	                        "ORDER BY group_position, item_position LIMIT 1");

	if (result->HasError()) {
		auto result_str = result->ToString();
		if (result_str.find("No such file or directory") == string::npos) {
			REQUIRE_NO_FAIL(*result);
		}
		CURRENT_CONFIG.failed_queries++;
	}
	manager.Detach(file_id, con);
}

static void QueryWorker(AttachManager &manager, DuckDB &db) {

	RandomEngine engine;
	Connection con(db);

	for (idx_t i = 0; i < CURRENT_CONFIG.query_count / CURRENT_CONFIG.worker_count; i++) {
		auto file_id = GetFileId(engine);
		Query(manager, con, file_id);
	}
}

static void Run() {

	DuckDB db(nullptr);
	AttachManager manager(db, CURRENT_CONFIG.lru_size);

	// disable some optimizers
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers TO 'JOIN_ORDER,STATISTICS_PROPAGATION';"));

	// loop setup
	std::vector<thread> threads;

	// spawn and run workers
	for (idx_t i = 0; i < CURRENT_CONFIG.worker_count; i++) {
		threads.emplace_back(QueryWorker, ref(manager), ref(db));
	}

	for (idx_t i = 0; i < CURRENT_CONFIG.worker_count; i++) {
		threads[i].join();
	}

	CURRENT_CONFIG.total_attached_files = manager.TotalAttachedFiles();
	CURRENT_CONFIG.avg_attached_files = manager.AvgAttachPerFile();
	CURRENT_CONFIG.max_attached_files = manager.MaxAttachPerFile();
}

TEST_CASE("Test ATTACH scenarios", "[attach][.]") {

	return;

	bool use_lru = true;
	std::vector<idx_t> skews = {0};
	std::vector<idx_t> ratios = {0};
	std::vector<idx_t> query_counts = {160000};
	std::vector<idx_t> file_counts = {80000};
	std::vector<idx_t> worker_counts = {10};
	std::vector<idx_t> lru_sizes = {80000};

	// create configurations
	std::vector<Config> configs;
	for (const auto &skew : skews) {
		Config config;
		config.use_lru = use_lru;
		config.skew = skew;

		for (const auto &ratio : ratios) {
			config.ratio = ratio;

			for (const auto &query_count : query_counts) {

				config.query_count = query_count;

				for (const auto &file_count : file_counts) {
					config.file_count = file_count;

					for (const auto &worker_count : worker_counts) {
						config.worker_count = worker_count;

						for (const auto &lru_size : lru_sizes) {
							config.lru_size = lru_size;
							configs.push_back(config);
						}
					}
				}
			}
		}
	}

	// run configurations
	cout << "permutations to run: " << configs.size() << "\n";
	for (auto &config : configs) {
		CURRENT_CONFIG = config;

		auto t1 = chrono::high_resolution_clock::now();
		Run();
		auto t2 = chrono::high_resolution_clock::now();

		auto ms = chrono::duration_cast<chrono::milliseconds>(t2 - t1);
		auto s = chrono::duration_cast<chrono::seconds>(t2 - t1);

		config = CURRENT_CONFIG;
		config.ms = ms.count();
		config.s = s.count();
		config.Print();
	}

	// print configurations as CSV
	cout << "\n";
	for (const auto &config : configs) {
		config.PrintCSV();
	}
}
