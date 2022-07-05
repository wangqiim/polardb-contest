#include <gtest/gtest.h>
#include "leveldb/db.h"

using namespace std;
using namespace leveldb;

TEST(LeveldbTest, Basic) {
    leveldb::DB *db;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, "testdb", &db);
    assert(status.ok());

    status = db->Put(WriteOptions(), "KeyNameExample", "ValueExample");
    assert(status.ok());
    string res;
    status = db->Get(ReadOptions(), "KeyNameExample", &res);
    assert(status.ok());
    status = db->Get(ReadOptions(), "KeyNameExampleSStable", &res);
    assert(status.IsNotFound());
    cout << res << endl;

    delete db;
}
