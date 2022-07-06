#include <gtest/gtest.h>
#include "interface.h"

class TestUser
{
public:
    int64_t id = 0;
    char user_id[128] = {0};
    char name[128] = {0};
    int64_t salary = 0;
};

enum TestColumn{Id=0,Userid,Name,Salary};

bool FileExists(const std::string& path) {
  return access(path.c_str(), F_OK) == 0;
}

int drop_datafile() {
    std::string path = "/tmp/polarDB/DATA";
    if (FileExists(path)) {
        return remove(path.c_str());
    }
    return 0;
}

TEST(InterfaceTest, Basic) {
    EXPECT_EQ(0, drop_datafile());
    TestUser user;
    user.id = 0;
    user.salary = 100;
    memcpy(&user.name,"hello",5);
    void* ctx = engine_init(nullptr, nullptr, 0, "/mnt/aep/", "/mnt/disk/");
    engine_write(ctx,&user,sizeof(user));
    char res[2000*128];
    size_t read_cnt = engine_read(ctx, Id, Name, &user.name, 8, res);

    EXPECT_EQ(1, read_cnt);
    EXPECT_EQ(0, *(int64_t *)res);
    
    engine_deinit(ctx);
    EXPECT_EQ(0, drop_datafile());
}

TEST(InterfaceTest, ManyUser) {
    EXPECT_EQ(0, drop_datafile());
    // user1 user2 salary 相同
    // user2 user3 user4 name 相同
    TestUser user1;
    user1.id = 1;
    memcpy(&user1.user_id, "user1", 5);
    memcpy(&user1.name, "name1", 5);
    user1.salary = 2;

    TestUser user2;
    user2.id = 2;
    memcpy(&user2.user_id, "user2", 5);
    memcpy(&user2.name, "name2", 5);
    user2.salary = 2;

    TestUser user3;
    user3.id = 3;
    memcpy(&user3.user_id, "user3", 5);
    memcpy(&user3.name, "name2", 5);
    user3.salary = 3;

    TestUser user4;
    user4.id = 4;
    memcpy(&user4.user_id, "user4", 5);
    memcpy(&user4.name, "name2", 5);
    user4.salary = 4;

    void* ctx = engine_init(nullptr, nullptr, 0, "/mnt/aep/", "/tmp");

    engine_write(ctx,&user1,sizeof(user1));
    engine_write(ctx,&user2,sizeof(user2));
    engine_write(ctx,&user3,sizeof(user3));
    engine_write(ctx,&user4,sizeof(user4));

    char res[2000*128];
    size_t read_cnt = engine_read(ctx, Id, Salary, &user1.salary, 8, res);
    EXPECT_EQ(2, read_cnt);

    read_cnt = engine_read(ctx, Id, Name, &user2.name, 128, res);
    EXPECT_EQ(3, read_cnt);

    engine_deinit(ctx);
    EXPECT_EQ(0, drop_datafile());
}

TEST(InterfaceTest, Replay) {
    EXPECT_EQ(0, drop_datafile());
    TestUser user1;
    user1.id = 1;
    memcpy(&user1.user_id, "user1", 5);
    memcpy(&user1.name, "name1", 5);
    user1.salary = 2;

    TestUser user2;
    user2.id = 2;
    memcpy(&user2.user_id, "user2", 5);
    memcpy(&user2.name, "name2", 5);
    user2.salary = 2;

    TestUser user3;
    user3.id = 3;
    memcpy(&user3.user_id, "user3", 5);
    memcpy(&user3.name, "name2", 5);
    user3.salary = 3;

    TestUser user4;
    user4.id = 4;
    memcpy(&user4.user_id, "user4", 5);
    memcpy(&user4.name, "name2", 5);
    user4.salary = 4;

    void* ctx = engine_init(nullptr, nullptr, 0, "/mnt/aep/", "/tmp");

    engine_write(ctx,&user1,sizeof(user1));
    engine_write(ctx,&user2,sizeof(user2));
    engine_write(ctx,&user3,sizeof(user3));
    engine_write(ctx,&user4,sizeof(user4));

    char res[2000*128];
    size_t read_cnt = engine_read(ctx, Id, Salary, &user1.salary, 8, res);
    EXPECT_EQ(2, read_cnt);

    read_cnt = engine_read(ctx, Id, Name, &user2.name, 128, res);
    EXPECT_EQ(3, read_cnt);

    engine_deinit(ctx);

    ctx = engine_init(nullptr, nullptr, 0, "/mnt/aep/", "/tmp");
    read_cnt = engine_read(ctx, Id, Salary, &user1.salary, 8, res);
    EXPECT_EQ(2, read_cnt);

    read_cnt = engine_read(ctx, Id, Name, &user2.name, 128, res);
    EXPECT_EQ(3, read_cnt);
    engine_deinit(ctx);
    EXPECT_EQ(0, drop_datafile());
}