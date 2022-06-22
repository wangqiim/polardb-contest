#include "../inc/interface.h"
#include <iostream>
#include <vector>
#include <string.h>

class User
{
public:
    int64_t id;
    char user_id[128];
    char name[128];
    int64_t salary;
};

enum Column{Id=0,Userid,Name,Salary};

std::vector<User> users;

void engine_write( void *ctx, const void *data, size_t len) {
    User user;
    memcpy(&user,data,len);
    users.push_back(user);
 }

size_t engine_read( void *ctx, int32_t select_column,
    int32_t where_column, const void *column_key, size_t column_key_len, void *res) {
    int users_size = users.size(); 
    bool b = true;
    size_t res_num = 0;
    for(int i=0;i<users_size;++i)
    {
        switch(where_column) {
            case Id: b = memcmp(column_key,&users[i].id,column_key_len) == 0; break;
            case Userid: b = memcmp(column_key,users[i].user_id,column_key_len) == 0; break;
            case Name: b = memcmp(column_key,users[i].name,column_key_len) == 0; break;
            case Salary: b = memcmp(column_key,&users[i].salary,column_key_len) == 0; break;
            default: b = false; break; // wrong
        }
        if(b)
        {
            ++res_num;
            switch(select_column) {
                case Id: 
                    memcpy(res, &users[i].id, 8); 
                    res = (char *)res + 8; 
                    break;
                case Userid: 
                    memcpy(res, users[i].user_id, 128); 
                    res = (char *)res + 128; 
                    break;
                case Name: 
                    memcpy(res, users[i].name, 128); 
                    res = (char *)res + 128; 
                    break;
                case Salary: 
                    memcpy(res, &users[i].salary, 8); 
                    res = (char *)res + 8; 
                    break;
                default: break; // wrong
            }
        }
    }
    return res_num;
}

void* engine_init(const char* host_info, const char* const* peer_host_info, size_t peer_host_info_num,
                  const char* aep_dir, const char* disk_dir) {return nullptr;}

void engine_deinit(void *ctx) {}
