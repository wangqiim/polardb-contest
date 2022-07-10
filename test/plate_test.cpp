#include <gtest/gtest.h>
#include "plate.h"
#include "test_util.h"
#include "spdlog/spdlog.h"

class Reader {
    public:
        void read(TestUser *user) { cnt_++; }
        int get_cnt() { return cnt_; }
        void reset() { cnt_ = 0; }
    private:
        int cnt_ = 0;
};

void read_record(void *record, void *location, void *context) {
    TestUser *user = reinterpret_cast<TestUser *>(record);
    Reader *reader = reinterpret_cast<Reader *>(context);
    reader->read(user);
}

TEST(PlateTest, Basic) {
    EXPECT_EQ(0, rmtree(disk_dir));
    Plate *plate = new Plate(disk_dir);
    plate->Init();

    EXPECT_EQ(0, plate->size());

    TestUser user;
    user.id = 0;
    user.salary = 100;
    memcpy(&user.user_id,"12345",5);
    memcpy(&user.name,"hello",5);

    Location loc;
    int ret = plate->append(reinterpret_cast<void *>(&user), loc);
    EXPECT_EQ(0, ret);
    EXPECT_EQ(1, plate->size());
    EXPECT_EQ(0, loc.file_id_);
    EXPECT_EQ(0, loc.offset_);
    
    char res[2000*128];
    Reader reader;
    EXPECT_EQ(0, reader.get_cnt());
    ret = plate->scan(read_record, reinterpret_cast<void *>(&reader));
    EXPECT_EQ(0, ret);
    EXPECT_EQ(1, reader.get_cnt());

    TestUser user_get;
    ret = plate->get(loc, (void*)&user_get);
    EXPECT_EQ(0, ret);
    EXPECT_EQ(true, user_get == user);
    spdlog::info("user: {}", user.to_string());
    spdlog::info("user_get: {}", user_get.to_string());

    user.id = 1;
    ret = plate->append(reinterpret_cast<void *>(&user), loc);
    EXPECT_EQ(0, ret);
    reader.reset();
    ret = plate->scan(read_record, reinterpret_cast<void *>(&reader));
    EXPECT_EQ(0, ret);
    EXPECT_EQ(2, reader.get_cnt());
    EXPECT_EQ(0, loc.file_id_);
    EXPECT_EQ(1, loc.offset_);
    ret = plate->get(loc, (void*)&user_get);
    EXPECT_EQ(0, ret);
    EXPECT_EQ(true, user_get == user);
    spdlog::info("user: {}", user.to_string());
    spdlog::info("user_get: {}", user_get.to_string());
    delete plate;
    EXPECT_EQ(0, rmtree(disk_dir));
}

TEST(PlateTest, openFileNum) {
    EXPECT_EQ(0, rmtree(disk_dir));
    Plate *plate = new Plate(disk_dir);
    plate->Init();

    EXPECT_EQ(0, plate->size());

    TestUser user;
    user.id = 0;
    user.salary = 100;
    memcpy(&user.user_id,"12345",5);
    memcpy(&user.name,"hello",5);

    int ret = 0;

    Location loc;
    int write_cnt = MINIRECORDNUM * 5;

    for (int i = 0; i < write_cnt; i++) {
        int ret = plate->append(reinterpret_cast<void *>(&user), loc);
        EXPECT_EQ(0, ret);
    }
    EXPECT_EQ(write_cnt, plate->size());
    EXPECT_EQ(1, plate->openFileNum());

    Reader reader;
    EXPECT_EQ(0, reader.get_cnt());
    ret = plate->scan(read_record, reinterpret_cast<void *>(&reader));
    EXPECT_EQ(0, ret);
    EXPECT_EQ(write_cnt, reader.get_cnt());
    EXPECT_EQ(1, plate->openFileNum());

    delete plate;

    plate = new Plate(disk_dir);
    plate->Init();

    EXPECT_EQ(write_cnt, plate->size());
    EXPECT_EQ(1, plate->openFileNum());
    reader.reset();
    EXPECT_EQ(0, reader.get_cnt());
    ret = plate->scan(read_record, reinterpret_cast<void *>(&reader));
    EXPECT_EQ(0, ret);
    EXPECT_EQ(MINIRECORDNUM * 5, reader.get_cnt());
    EXPECT_EQ(1, plate->openFileNum());

    delete plate;
    EXPECT_EQ(0, rmtree(disk_dir));
}
