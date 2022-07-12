#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fstream>

#include "spdlog/spdlog.h"
#include "engine.h"
#include "util.h"

thread_local int tid_ = -1;

void add_res(const User &user, int32_t select_column, void **result) {
  switch(select_column) {
    case Id: 
      memcpy(*result, &user.id, 8); 
      *result = (char *)(*result) + 8; 
      break;
    case Userid: 
      memcpy(*result, user.user_id, 128); 
      *result = (char *)(*result)  + 128; 
      break;
    case Name: 
      memcpy(*result, user.name, 128); 
      *result = (char *)(*result) + 128; 
      break;
    case Salary: 
      memcpy(*result, &user.salary, 8); 
      *result = (char *)(*result) + 8; 
      break;
    default: 
      spdlog::error("unexpected here");
      break;
  }
}

// ------Index Builder-------------
class Index_Builder {
  public:
    Index_Builder(primary_key *idx_id,
      unique_key *idx_user_id, normal_key  *idx_salary)
      : idx_id_(idx_id),
        idx_user_id_(idx_user_id),
        idx_salary_(idx_salary) { }

    void build(int log_id, const User *user);

    primary_key *idx_id_;
    unique_key  *idx_user_id_;
    normal_key  *idx_salary_;
};

void Index_Builder::build(int log_id, const User *user) {
  // build pk index
  idx_id_[log_id].insert({user->id, *user});
  // build uk index
  idx_user_id_[log_id].insert({UserIdWrapper(user->user_id), user->id});
  // build nk index
  idx_salary_[log_id].insert({user->salary,user->id});
}

// --------------------Engine-----------------------------
Engine::Engine(const char* aep_dir, const char* disk_dir)
  : next_tid_(0), mtx_(), aep_dir_(aep_dir), dir_(disk_dir), log_(),
    idx_id_list_(), idx_user_id_list_(), idx_salary_list_(), phase_(Phase::Hybrid) {}

Engine::~Engine() {
  for (size_t i = 0; i < log_.size(); i++) {
    delete log_[i];
  }
  file_paths_.clear();
  log_.clear();
  auto end = std::chrono::system_clock::now();
  std::chrono::duration<double> elapsed_seconds = end-start_;
  spdlog::info("since init done, elapsed time: {}s", elapsed_seconds.count());
}

int Engine::Init() {
  if (!Util::FileExists(aep_dir_)) {
    spdlog::info("aep_dir_: {} path is not exist, start to create it", aep_dir_);
  }
  // create dir
  if (!Util::FileExists(aep_dir_) && 0 != mkdir(aep_dir_.c_str(), 0755)) {
    spdlog::error("init create dir[{}] fail!", aep_dir_);
    return -1;
  }
  if (!Util::FileExists(dir_)) {
    spdlog::info("dir_: {} path is not exist, start to create it", dir_);
  }
  // create dir
  if (!Util::FileExists(dir_) && 0 != mkdir(dir_.c_str(), 0755)) {
    spdlog::error("init create dir[{}] fail!", dir_);
    return -1;
  }

  // build index
  Util::gen_sorted_paths(dir_, kWALFileName, file_paths_, SSDNum);
  Util::gen_sorted_paths(aep_dir_, kWALFileName, file_paths_, AEPNum);
  int record_num = Util::evaluate_files_record_nums(file_paths_, RecordSize);
  if (record_num == -1) {
    spdlog::error("evaluate record num fail");
    return -1;
  }
  // hack test phase
  spdlog::info("evaluate record num = {}", record_num);
  if (record_num == 0) {
    phase_ = Phase::WriteOnly;
    spdlog::info("WriteOnly mode init, start replay index");
    pre_reserve(ShardNum, WritePerClient);
    record_num = replay_index(file_paths_);
  } else if (record_num == WritePerClient * ClientNum) { // if is Phase::ReadOnly, only build index in one map
    phase_ = Phase::ReadOnly; 
    spdlog::info("ReadOnly mode init, start replay index");
    pre_reserve(1, WritePerClient * ClientNum);
    record_num = readOnly_replay_index(file_paths_);
  } else {
    phase_ = Phase::Hybrid;
    spdlog::info("Hybrid mode init, start replay index");
    pre_reserve(ShardNum, WritePerClient);
    record_num = replay_index(file_paths_);
  }
  spdlog::info("replay build index done, record num = {}", record_num);
  
  PosixWritableFile *walfile = nullptr;
  for (const auto &fname: file_paths_) {
    int ret = PosixEnv::NewAppendableFile(fname, &walfile);
    if (ret != 0) {
      return -1;
    }
    log_.push_back(new Writer(walfile));
  }
  Util::print_resident_set_size();
  spdlog::info("engine init done");
  start_ = std::chrono::system_clock::now();
  return 0;
}

int Engine::Append(const void *datas) {
  must_set_tid();
  if (phase_ == Phase::Hybrid) {
    mtx_.lock();
  }
  const User *user = reinterpret_cast<const User *>(datas);

  log_[tid_]->AddRecord(datas, RecordSize);

  idx_id_list_[tid_].insert({user->id, *user});
  // build uk index
  idx_user_id_list_[tid_].emplace(user->user_id, user->id); // avoid unneccessary copy constructer

  // build nk index
  idx_salary_list_[tid_].insert({user->salary, user->id});
  
  if (phase_ == Phase::Hybrid) {
    mtx_.unlock();
  }
  return 0;
}

size_t Engine::Read(void *ctx, int32_t select_column,
    int32_t where_column, const void *column_key, 
    size_t column_key_len, void *res) {

  must_set_tid();
  if (phase_ == Phase::Hybrid) {
    mtx_.lock();
  } else if (phase_ == Phase::ReadOnly) {
    return readOnly_read(ctx, select_column, where_column, column_key, column_key_len, res);
  }
  spdlog::debug("[engine_read] [select_column:{0:d}] [where_column:{1:d}] [column_key_len:{2:d}]", select_column, where_column, column_key_len); 
  User user;
  size_t res_num = 0;
  switch(where_column) {
      case Id: {
        if (column_key_len != 8) {
          spdlog::error("read column_key_len is: {}, expcted: 8", column_key_len);
        }
        int64_t id = *((int64_t *)column_key);
        for (int i = 0; i < ShardNum; i++) {
          auto iter = idx_id_list_[i].find(id);
          if (iter != idx_id_list_[i].end()) {
            res_num = 1;
            user = iter->second;
            add_res(user, select_column, &res);
            break;
          }
        }
      }
      break;

      case Userid: {
        if (column_key_len != 128) {
          spdlog::error("read column_key_len is: {}, expcted: 128", column_key_len);
        }

        for (int i = 0; i < ShardNum; i++) {
          auto iter = idx_user_id_list_[i].find(UserIdWrapper((const char *)column_key));
          if (iter != idx_user_id_list_[i].end()) {
            res_num = 1;
            int64_t id = iter->second;
            for (int j = 0; j < ShardNum; j++) {
              auto iter_id = idx_id_list_[j].find(id);
              if (iter_id != idx_id_list_[j].end()) {
                user = iter_id->second;
                add_res(user, select_column, &res);
                break;
              }
            }
            break;
          }
        }
      } 
      break;

      case Name: {
        spdlog::error("don't support select where column[Name]");
      } 
      break;

      case Salary: {
        int64_t salary = *((int64_t *)column_key);
        if (column_key_len != 8) {
          spdlog::error("read column_key_len is: {}, expcted: 8", column_key_len);
        }
        for (int i = 0; i < ShardNum; i++) {
          auto range = idx_salary_list_[i].equal_range(salary);
          auto iter = range.first;
          while (iter != range.second) {
            res_num += 1;
            int64_t id = iter->second;
            for (int j = 0; j < ShardNum; j++) {
              auto iter_id = idx_id_list_[j].find(id);
              if (iter_id != idx_id_list_[j].end()) {
                user = iter_id->second;
                add_res(user, select_column, &res);
                break;
              }
            }
            iter++;
          }
        }
      }
      break;

      default:
        spdlog::error("unexpected where_column: {}", where_column);
      break;
  }
  if (phase_ == Phase::Hybrid) {
    mtx_.unlock();
  }
  return res_num;
}

int Engine::replay_index(const std::vector<std::string> paths) {
  Index_Builder index_builder(idx_id_list_, idx_user_id_list_, idx_salary_list_);
  int cnt = 0;
  for (size_t log_id = 0; log_id < paths.size(); log_id++) {
    bool new_create = false;
    if (!Util::FileExists(paths[log_id])) {
      int fd = open(paths[log_id].c_str(), O_RDWR | O_CREAT, 0644);
      if (fd >= 0) {
        new_create = true;
        spdlog::info("init create log[{}] success!", paths[log_id]);
        close(fd);
      } else {
        spdlog::error("init create log[{}] fail!", dir_);
        return -1;
      }
    }
    if (!new_create) {
      PosixSequentialFile *file = nullptr;
      int ret = PosixEnv::NewSequentialFile(paths[log_id], &file);
      if (ret == 0) {
        // log is exist, need recovery
        Reader reader(file); // 离开作用域时，回调用reader的析构函数, file的内存由Reader管理
        std::string record;
        while (reader.ReadRecord(record, RecordSize)) {
          const User *user = (const User *)record.data();
          index_builder.build(log_id, user);
          cnt++;
        }
      }
    }
  }
  return cnt;
}

inline int Engine::must_set_tid() {
  if (tid_ == -1) {
    tid_ = next_tid_.fetch_add(1);
    if (tid_ >= ShardNum) {
      spdlog::warn("w/r thread excceed 50!!");
      tid_ %= ShardNum;
    }
  }
  return 0;
} 

// -------------------------------readOnly mod api-----------------------------------------
int Engine::readOnly_replay_index(const std::vector<std::string> paths) {
  for (auto &idx: idx_id_list_) {
    idx.clear();
  }
  for (auto &idx: idx_user_id_list_) {
    idx.clear();
  }
  for (auto &idx: idx_salary_list_) {
    idx.clear();
  }
  Index_Builder index_builder(idx_id_list_, idx_user_id_list_, idx_salary_list_);
  int cnt = 0;
  for (size_t log_id = 0; log_id < paths.size(); log_id++) {
    bool new_create = false;
    if (!Util::FileExists(paths[log_id])) {
      int fd = open(paths[log_id].c_str(), O_RDWR | O_CREAT, 0644);
      if (fd >= 0) {
        new_create = true;
        spdlog::info("init create log[{}] success!", paths[log_id]);
        close(fd);
      } else {
        spdlog::error("init create log[{}] fail!", dir_);
        return -1;
      }
    }
    if (!new_create) {
      PosixSequentialFile *file = nullptr;
      int ret = PosixEnv::NewSequentialFile(paths[log_id], &file);
      if (ret == 0) {
        // log is exist, need recovery
        Reader reader(file);// 离开作用域时，回调用reader的析构函数, file的内存由Reader管理
        std::string record;
        while (reader.ReadRecord(record, RecordSize)) {
          const User *user = (const User *)record.data();
          index_builder.build(0, user); // readonly only use map[0] insteads of [0, 49]
          cnt++;
        }
      }
    }
  }
  return cnt;
}

size_t Engine::readOnly_read(void *ctx, int32_t select_column,
    int32_t where_column, const void *column_key, 
    size_t column_key_len, void *res) {

  size_t res_num = 0;
  switch(where_column) {
      case Id: {
        int64_t id = *((int64_t *)column_key);
        auto iter = idx_id_list_[0].find(id);
        if (iter != idx_id_list_[0].end()) {
          res_num = 1;
          add_res(iter->second, select_column, &res);
        }
      }
      break;

      case Userid: {
        auto iter = idx_user_id_list_[0].find(UserIdWrapper((const char *)column_key));
        res_num = 1;
        int64_t id = iter->second;
        if (select_column == Id) {
          // 无需回表
          memcpy(res, &id, 8); 
          res = (char *)res + 8;      
        } else {
          add_res(idx_id_list_[0].find(id)->second, select_column, &res);
        }
      } 
      break;

      case Name: {
        spdlog::error("don't support select where column[Name]");
      } 
      break;

      case Salary: {
        int64_t salary = *((int64_t *)column_key);
        auto range = idx_salary_list_[0].equal_range(salary);
        auto iter = range.first;
        while (iter != range.second) {
          res_num += 1;
          int64_t id = iter->second;
          if (select_column == Id) {
            memcpy(res, &id, 8); 
            res = (char *)res + 8;
          } else {
            add_res(idx_id_list_[0].find(id)->second, select_column, &res);
          }
          iter++;
        }
      }
      break;

      default:
        spdlog::error("unexpected where_column: {}", where_column);
      break;
  }
  return res_num;
}

int Engine::pre_reserve(int n, size_t count) {
  for (int i = 0; i < n; i++) {
    idx_id_list_[i].reserve(count);
    idx_user_id_list_[i].reserve(count);
  }
  return 0;
}
