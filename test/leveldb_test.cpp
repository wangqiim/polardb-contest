#include <gtest/gtest.h>
#include "leveldb/db.h"
#include "leveldb/write_batch.h"

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

TEST(LeveldbTest, WriteBatch) {
    leveldb::DB *db;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, "testdb", &db);
    assert(status.ok());

    leveldb::WriteBatch wb;

    wb.Put("k1", "v1");
    wb.Put("k2", "v2");

    db->Write(leveldb::WriteOptions(), &wb);
    std::string v1;
    status = db->Get(leveldb::ReadOptions(), "k1", &v1);
    assert(status.ok());
    assert(v1 == "v1");

    std::string v2;
    status = db->Get(leveldb::ReadOptions(), "k2", &v2);
    assert(status.ok());
    assert(v2 == "v2");
    delete db;
}
