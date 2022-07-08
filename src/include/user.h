#include <stdint.h>
#include <stdio.h>
#include <string>

uint32_t StrHash(const char* s, int size);

class User {
public:
    int64_t id = 0;
    char user_id[128] = {0};
    char name[128] = {0};
    int64_t salary = 0;
    
    std::string to_string() {
        char buf[500] = {0};
        sprintf(buf, "id: %lld, user_id: %u, name: %u, salary: %lld",
            (long long)id, StrHash(user_id, 128), StrHash(name, 128), (long long)salary);
        return std::string(buf);
    }
};

enum Column{Id=0,Userid,Name,Salary};