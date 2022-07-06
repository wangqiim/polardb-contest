#include "../inc/interface.h"
#include <iostream>
#include <vector>
#include <string.h>
#include "spdlog/spdlog.h"
#include "plate.h"

class User
{
public:
    int64_t id;
    char user_id[128];
    char name[128];
    int64_t salary;
};

enum Column{Id=0,Userid,Name,Salary};

Plate *plate_engine;


class EngineReader {
  public:
      EngineReader(int32_t select_column, int32_t where_column, const void *column_key, size_t column_key_len, void *res)
        : select_column_(select_column), 
        where_column_(where_column), 
        column_key_(column_key), 
        column_key_len_(column_key_len),
        res_(res),
        res_num_(0) {}
      
      void   read(User *user);
      size_t get_cnt() { return res_num_; }
      void   reset() { res_num_ = 0; }
  private:
    int32_t select_column_;
    int32_t where_column_;
    const void *column_key_;
    size_t column_key_len_;
    void *res_;

    size_t  res_num_;
};

void EngineReader::read(User *user) {
  bool b = true;
  switch(where_column_) {
      case Id: b = memcmp(column_key_, &user->id, column_key_len_) == 0; break;
      case Userid: b = memcmp(column_key_, user->user_id, column_key_len_) == 0; break;
      case Name: b = memcmp(column_key_, user->name, column_key_len_) == 0; break;
      case Salary: b = memcmp(column_key_, &user->salary, column_key_len_) == 0; break;
      default: b = false; break; // wrong
  }
  if(b)
  {
      ++res_num_;
      switch(select_column_) {
          case Id: 
              memcpy(res_, &user->id, 8); 
              res_ = (char *)res_ + 8; 
              break;
          case Userid: 
              memcpy(res_, user->user_id, 128); 
              res_ = (char *)res_ + 128; 
              break;
          case Name: 
              memcpy(res_, user->name, 128); 
              res_ = (char *)res_ + 128; 
              break;
          case Salary: 
              memcpy(res_, &user->salary, 8); 
              res_ = (char *)res_ + 8; 
              break;
          default: 
          spdlog::error("unexpected here");
          break; // wrong
      }
  }
}

void read_record(void *record, void *context) {
    User *user = reinterpret_cast<User *>(record);
    EngineReader *reader = reinterpret_cast<EngineReader *>(context);
    reader->read(user);
}

void engine_write( void *ctx, const void *data, size_t len) {
    User user;
    memcpy(&user,data,len);
    if (len != RECORDSIZE) {
      spdlog::error("engine_write len not equal to {:d}", RECORDSIZE);
    }
    plate_engine->append(data);
 }

size_t engine_read( void *ctx, int32_t select_column,
    int32_t where_column, const void *column_key, size_t column_key_len, void *res) {
    spdlog::debug("[engine_read] [select_column:{0:d}] [where_column:{1:d}] [column_key_len:{2:d}]", select_column, where_column, column_key_len); 
    bool b = true;
    size_t res_num = 0;
    EngineReader reader(select_column, where_column, column_key, column_key_len, res);
    plate_engine->scan(read_record, reinterpret_cast<void *>(&reader));
    return reader.get_cnt();
}

void* engine_init(const char* host_info, const char* const* peer_host_info, size_t peer_host_info_num,
                  const char* aep_dir, const char* disk_dir) {
    spdlog::set_level(spdlog::level::debug);
    spdlog::info("[plate engine_init]");
    plate_engine = new Plate("/tmp/polarDB");
    plate_engine->Init();
    return nullptr;
}

void engine_deinit(void *ctx) {
    spdlog::info("[plate engine_deinit]");
    plate_engine->~Plate();
}
