#include <stdint.h>
#include <stdio.h>

class User {
public:
    int64_t id = 0;
    char user_id[128] = {0};
    char name[128] = {0};
    int64_t salary = 0;
};

enum Column{Id=0,Userid,Name,Salary};