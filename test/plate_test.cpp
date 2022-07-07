#include <gtest/gtest.h>
#include "plate.h"
#include "test_util.h"

class TestUser {
    public:
        int64_t id;
        char user_id[128];
        char name[128];
        int64_t salary;
};

enum TestColumn{Id=0,Userid,Name,Salary};

class Reader {
    public:
        void read(TestUser *user) { cnt_++; }
        int get_cnt() { return cnt_; }
        void reset() { cnt_ = 0; }
    private:
        int cnt_ = 0;
};

void read_record(void *record, void *context) {
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

    ret = plate->append(reinterpret_cast<void *>(&user), loc);
    EXPECT_EQ(0, ret);
    reader.reset();
    ret = plate->scan(read_record, reinterpret_cast<void *>(&reader));
    EXPECT_EQ(0, ret);
    EXPECT_EQ(2, reader.get_cnt());
    EXPECT_EQ(0, loc.file_id_);
    EXPECT_EQ(1, loc.offset_);
    delete plate;
    EXPECT_EQ(0, rmtree(disk_dir));
}
