#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include "spdlog/spdlog.h"

uint32_t StrHash(const char* s, int size);

class User {
public:
    int64_t id = 0;
    char user_id[128] = {0};
    char name[128] = {0};
    int64_t salary = 0;
    
    bool operator==(const User &rhs) {
      return (id == rhs.id && salary == rhs.salary
          && memcmp(user_id, rhs.user_id, 128) == 0
          && memcmp(name, rhs.name, 128) == 0);
    }

    std::string to_string() {
        char buf[500] = {0};
        sprintf(buf, "id: %lld, user_id: %u, name: %u, salary: %lld",
            (long long)id, StrHash(user_id, 128), StrHash(name, 128), (long long)salary);
        return std::string(buf);
    }
};

class UserWithoutId {
public:
    char user_id[128] = {0};
    char name[128] = {0};
    int64_t salary = 0;
    
    bool operator==(const User &rhs) {
      return (salary == rhs.salary
          && memcmp(user_id, rhs.user_id, 128) == 0
          && memcmp(name, rhs.name, 128) == 0);
    }

    std::string to_string() {
        char buf[500] = {0};
        sprintf(buf, "user_id: %u, name: %u, salary: %lld", StrHash(user_id, 128), StrHash(name, 128), (long long)salary);
        return std::string(buf);
    }
};

enum Column{Id=0,Userid,Name,Salary};

const int UseridLen = 128;

class UserIdWrapper { // 比字符串作为key省不少空间
public:
  char s[UseridLen];
  char *ptr; // ptr->s[UseridLen] 减少map.find时候调用复制构造函数的开销
  UserIdWrapper(const char *t): ptr(s) {
    memcpy(s, t, UseridLen);
  }

  // when map.find, don't memcpy
  UserIdWrapper(const char *t, bool use_ptr): ptr(const_cast<char *>(t)) {
  }

  UserIdWrapper(const UserIdWrapper &rhs) {
    if (rhs.ptr == rhs.s) {
      ptr = s;
      memcpy(ptr, rhs.ptr, UseridLen);
    } else {
      // we don't have ptr resource, don't copy consrtruct. abort it!
      spdlog::error("we don't have ptr resource, don't copy consrtruct!");
      exit(1);
    }
  }

  bool operator== (const UserIdWrapper &other) const {
    return memcmp(ptr, other.ptr, UseridLen) == 0;
  }
  
  bool operator< (const UserIdWrapper &other) const {
    return memcmp(ptr, other.ptr, UseridLen) < 0;
  }
};

namespace std {
    template <>
    struct hash<UserIdWrapper> {
        size_t operator()(const UserIdWrapper &k) const{
          return StrHash(k.ptr, 16);
        }
    };
}
