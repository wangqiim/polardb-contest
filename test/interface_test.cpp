#include <gtest/gtest.h>
#include "plate.h"
#include "interface.h"
#include "test_util.h"
#include "spdlog/spdlog.h"

TEST(InterfaceTest, Basic) {
    EXPECT_EQ(0, rmtree(disk_dir));
    TestUser user;
    user.id = 0;
    user.salary = 100;
    memcpy(&user.name,"hello",5);
    void* ctx = engine_init(nullptr, nullptr, 0, "/mnt/aep/", disk_dir);
    engine_write(ctx,&user,sizeof(user));
    char res[100*128];
    size_t read_cnt = engine_read(ctx, Id, Name, &user.name, 8, res);

    EXPECT_EQ(1, read_cnt);
    EXPECT_EQ(0, *(int64_t *)res);
    
    engine_deinit(ctx);
    EXPECT_EQ(0, rmtree(disk_dir));
}

TEST(InterfaceTest, ManyUser) {
    EXPECT_EQ(0, rmtree(disk_dir));
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

    void* ctx = engine_init(nullptr, nullptr, 0, "/mnt/aep/", disk_dir);

    engine_write(ctx,&user1,sizeof(user1));
    engine_write(ctx,&user2,sizeof(user2));
    engine_write(ctx,&user3,sizeof(user3));
    engine_write(ctx,&user4,sizeof(user4));

    char res[100*128];
    size_t read_cnt = engine_read(ctx, Id, Salary, &user1.salary, 8, res);
    EXPECT_EQ(2, read_cnt);

    read_cnt = engine_read(ctx, Id, Name, &user2.name, 128, res);
    EXPECT_EQ(3, read_cnt);

    engine_deinit(ctx);
    EXPECT_EQ(0, rmtree(disk_dir));
}

TEST(InterfaceTest, BasicReplay) {
    EXPECT_EQ(0, rmtree(disk_dir));
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

    void* ctx = engine_init(nullptr, nullptr, 0, "/mnt/aep/", disk_dir);

    engine_write(ctx,&user1,sizeof(user1));
    engine_write(ctx,&user2,sizeof(user2));
    engine_write(ctx,&user3,sizeof(user3));
    engine_write(ctx,&user4,sizeof(user4));

    char res[100*128];
    size_t read_cnt = engine_read(ctx, Id, Salary, &user1.salary, 8, res);
    EXPECT_EQ(2, read_cnt);

    read_cnt = engine_read(ctx, Id, Name, &user2.name, 128, res);
    EXPECT_EQ(3, read_cnt);

    engine_deinit(ctx);
    // replay
    ctx = engine_init(nullptr, nullptr, 0, "/mnt/aep/", disk_dir);
    read_cnt = engine_read(ctx, Id, Salary, &user1.salary, 8, res);
    EXPECT_EQ(2, read_cnt);

    read_cnt = engine_read(ctx, Id, Name, &user2.name, 128, res);
    EXPECT_EQ(3, read_cnt);
    
    TestUser user5;
    user5.id = 4;
    memcpy(&user5.user_id, "user5", 5);
    memcpy(&user5.name, "name2", 5);
    user5.salary = 4;

    engine_write(ctx,&user5,sizeof(user5));
    read_cnt = engine_read(ctx, Id, Name, &user2.name, 128, res);
    EXPECT_EQ(4, read_cnt);

    engine_deinit(ctx);
    EXPECT_EQ(0, rmtree(disk_dir));
}

TEST(InterfaceTest, ManyWrite) {
    EXPECT_EQ(0, rmtree(disk_dir));
    // user1 user2 salary 相同
    // user2 user3 user4 name 相同
    TestUser user1;
    user1.id = 1;
    memcpy(&user1.user_id, "user1", 5);
    memcpy(&user1.name, "name1", 5);
    user1.salary = 2;

    void* ctx = engine_init(nullptr, nullptr, 0, "/mnt/aep/", disk_dir);

    int write_cnt = MINIRECORDNUM * 2;
    for (int i = 0; i < write_cnt; i++) {
        engine_write(ctx, &user1, sizeof(user1));
    }

    char res[write_cnt * 128];
    size_t read_cnt = engine_read(ctx, Id, Salary, &user1.salary, 8, res);
    EXPECT_EQ(write_cnt, read_cnt);

    engine_deinit(ctx);
    EXPECT_EQ(0, rmtree(disk_dir));
}

TEST(InterfaceTest, ManyWriteReplay) {
    EXPECT_EQ(0, rmtree(disk_dir));
    // user1 user2 salary 相同
    // user2 user3 user4 name 相同
    TestUser user1;
    memcpy(&user1.user_id, "user1", 5);
    memcpy(&user1.name, "name1", 5);
    user1.salary = 2;

    void* ctx = engine_init(nullptr, nullptr, 0, "/mnt/aep/", disk_dir);

    int write_cnt = MINIRECORDNUM * 2;
    for (int i = 0; i < write_cnt; i++) {
        user1.id = i;
        engine_write(ctx, &user1, sizeof(user1));
    }

    char res[write_cnt * 128];
    size_t read_cnt = engine_read(ctx, Id, Salary, &user1.salary, 8, res);
    EXPECT_EQ(write_cnt, read_cnt);

    engine_deinit(ctx);

    ctx = engine_init(nullptr, nullptr, 0, "/mnt/aep/", disk_dir);
    read_cnt = engine_read(ctx, Id, Salary, &user1.salary, 8, res);
    EXPECT_EQ(write_cnt, read_cnt);

    user1.id = 0;
    engine_write(ctx, &user1, sizeof(user1));
    read_cnt = engine_read(ctx, Id, Salary, &user1.salary, 8, res);
    EXPECT_EQ(write_cnt + 1, read_cnt);

    engine_deinit(ctx);
    EXPECT_EQ(0, rmtree(disk_dir));
    EXPECT_EQ(0, rmtree(disk_dir));
}
