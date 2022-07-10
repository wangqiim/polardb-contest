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

const int UseridLen = 128;

class UserIdWrapper { // 比字符串作为key省不少空间
public:
  char s[UseridLen];
  UserIdWrapper(const char *t) {
    for (int i = 0; i < UseridLen; i++) s[i] = t[i];
  }

  bool operator== (const UserIdWrapper &other) const {
    for (int i = 0; i < UseridLen; i++) {
      if (s[i] != other.s[i]) {
        return false;
      }
    }
    return true;
  }
};

namespace std {
    template <>
    struct hash<UserIdWrapper> {
        size_t operator()(const UserIdWrapper &k) const{
          return StrHash(k.s, UseridLen);
        }
    };
}