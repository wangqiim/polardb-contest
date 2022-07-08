#include <stdint.h>
#include <stdio.h>

class User {
public:
    int64_t id = 0;
    char user_id[128] = {0};
    char name[128] = {0};
    int64_t salary = 0;
    
    std::string to_string() {
        char buf[500] = {0};
        sprintf(buf, "id: %lld, user_id: %s, name: %s, salary: %lld",
            (long long)id, user_id, name, (long long)salary);
        return std::string(buf);
    }
};

enum Column{Id=0,Userid,Name,Salary};