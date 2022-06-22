#include "../inc/interface.h"
#include <iostream>
#include <string.h>

class TestUser
{
public:
    int64_t id;
    char user_id[128];
    char name[128];
    int64_t salary;
};

enum TestColumn{Id=0,Userid,Name,Salary};

int main()
{
    TestUser user;
    user.id = 0;
    user.salary = 100;
    // memcpy(&user.name,"1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567",128);
    memcpy(&user.name,"hello",5);
    void* ctx = engine_init(nullptr, nullptr, 0, "/mnt/aep/", "/mnt/disk/");
    engine_write(ctx,&user,sizeof(user));
    int64_t id = 0;
    char res[2000*128];
    std::cout<<engine_read(ctx, Id, Name, &user.name, 8, res)<<std::endl;
    char res_name[128];
    std::cout<<"res:"<<*(int64_t *)res;
    engine_deinit(ctx);
}