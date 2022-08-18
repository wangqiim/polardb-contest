#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include <unordered_dense.h>

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
  
  bool operator< (const UserIdWrapper &other) const {
    return memcmp(s, other.s, UseridLen) < 0;
  }
};

namespace std {
    template <>
    struct hash<UserIdWrapper> {
        size_t operator()(const UserIdWrapper &k) const{
          return ankerl::unordered_dense::detail::wyhash::hash(k.s, UseridLen);
        }
    };
}


const int NameLen = 128;
class NameWrapper { // 比字符串作为key省不少空间
public:
  char s[NameLen];
  NameWrapper(const char *t) {
    for (int i = 0; i < NameLen; i++) s[i] = t[i];
  }
};

class LocationsWrapper {
public:
  LocationsWrapper(): size_(0), next_(nullptr) {};
  ~LocationsWrapper() {
    if (next_) {
      delete next_;
    }
  }

  LocationsWrapper(LocationsWrapper &&others) {
    loc_ = others.loc_;
    size_ = others.size_;
    next_ = others.next_;
    others.loc_ = 0;
    others.size_ = 0;
    others.next_ = nullptr;
  };

  LocationsWrapper(const LocationsWrapper&) = delete;
  LocationsWrapper& operator=(const LocationsWrapper& other) {
    if (next_) {
      delete next_;
      next_ = nullptr;
    }
    loc_ = other.loc_;
    size_ = other.size_;
    next_ = other.next_;
    return *this;
  }

  void Push(size_t loc) {
    if (0 == size_) {
      loc_ = loc;
    } else {
      if (size_ == 1) {
        next_ = new std::vector<size_t>();
      }
      next_->push_back(loc);
    }
    size_++;
  }

  size_t Size() { return size_; }

  // without bound check!!
  size_t& operator[](size_t index) {
    if (index == 0) {
      return loc_;
    }
    return next_->at(index - 1);
  }

private:
  size_t loc_;
  size_t size_;
  std::vector<size_t> *next_;
};

class BlizardHashWrapper {
public:
  BlizardHashWrapper(const char *str, __attribute__((unused))size_t len)
    : hash1_(*(const int64_t *)str) {
  }

  BlizardHashWrapper(BlizardHashWrapper &&other) {
    hash1_ = other.hash1_;
  }

  BlizardHashWrapper& operator=(const BlizardHashWrapper &other) {
    hash1_ = other.hash1_;
    return *this;
  }

  size_t Hash() const { return hash1_; }

  bool operator==(const BlizardHashWrapper &other) const {
    return hash1_ == other.hash1_;
  }
private:
  size_t hash1_;
};

namespace std {
    template <>
    struct hash<BlizardHashWrapper> {
        size_t operator()(const BlizardHashWrapper &k) const{
          return k.Hash();
        }
    };
}
