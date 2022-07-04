#include <gtest/gtest.h>
#include "interface.h"

class TestUser
{
public:
    int64_t id;
    char user_id[128];
    char name[128];
    int64_t salary;
};

enum TestColumn{Id=0,Userid,Name,Salary};

TEST(InterfaceTest, Basic) {
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
}
