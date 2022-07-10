#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fstream>

#include "spdlog/spdlog.h"
#include "engine.h"
#include "util.h"

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

    void build(const User *user);

    primary_key *idx_id_;
    unique_key  *idx_user_id_;
    normal_key  *idx_salary_;
};

void Index_Builder::build(const User *user) {
  // build pk index
  idx_id_[((uint32_t)user->id) % ShardNum].insert({user->id, *user});
  // build uk index
  size_t hid = StrHash(user->user_id, sizeof(user->user_id));
  idx_user_id_[hid % ShardNum].insert({UserIdWrapper(user->user_id), user->id});
  // build nk index
  idx_salary_[((uint32_t)user->salary) % ShardNum].insert({user->salary,user->id});
}

// --------------------Engine-----------------------------
Engine::Engine(const char* disk_dir)
  : dir_(disk_dir), log_(),
    idx_id_list_(), idx_user_id_list_(), idx_salary_list_() {}

Engine::~Engine() {
  for (int i = 0; i < log_.size(); i++) {
    delete (log_[i]->GetFile());
    delete log_[i];
  }
  spdlog::info("afger delete log");
  file_paths_.clear();
  log_.clear();
}

int Engine::Init() {
  spdlog::info("engine start init");
  if (!Util::FileExists(dir_)) {
    spdlog::info("{} path is not exist", dir_);
  }
  // create dir
  if (!Util::FileExists(dir_) && 0 != mkdir(dir_.c_str(), 0755)) {
    spdlog::error("init create dir[{}] fail!", dir_);
    return -1;
  }

  // build index
  Util::gen_sorted_paths(dir_, kWALFileName, file_paths_, WALNum);
  spdlog::info("start replay index");
  int record_num = replay_index(file_paths_);
  if (record_num == -1) {
    spdlog::error("replay build index fail");
    return -1;
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
  return 0;
}

int Engine::Append(const void *datas) {

  const User *user = reinterpret_cast<const User *>(datas);
  uint32_t log_hash_id = ((uint32_t)user->id) % WALNum;

  log_mtx_list_[0].lock();
  if ((++write_cnt_) % 1000000 == 0) {
    spdlog::info("[wangqiim] write {}", write_cnt_);
    Util::print_resident_set_size();
  } else if (write_cnt_ > 47000000) {
    if (write_cnt_ % 100000 == 0) {
      spdlog::info("[wangqiim] write {}", write_cnt_);
      Util::print_resident_set_size();
    }
  }
  log_[log_hash_id]->AddRecord(datas, RecordSize);
  log_mtx_list_[0].unlock();

  // build pk index
  uint32_t id1 = ((uint32_t)user->id) % ShardNum;
  uint32_t id2 = (StrHash(user->user_id, sizeof(user->user_id))) % ShardNum;
  uint32_t id3 = ((uint32_t)user->salary) % ShardNum;

  //  if (idx_id_list_[hid]->count(user->id) != 0) {
//    spdlog::error("insert dup id: {}", user->id);
//  } else if (idx_user_id_list_.count(std::string(user->user_id, 128)) != 0) {
//    spdlog::error("insert dup user_id: {}", user->user_id);
//  }

  idx_id_mtx_list_[id1].lock();
  idx_id_list_[id1].insert({user->id, *user});
  idx_id_mtx_list_[id1].unlock();
  // build uk index
  idx_user_id_mtx_list_[id2].lock();
  idx_user_id_list_[id2].insert({UserIdWrapper(user->user_id), user->id}); // must use string(char* s, size_t n) construct funciton
  idx_user_id_mtx_list_[id2].unlock();

  // build nk index
  idx_salary_mtx_list_[id3].lock();
  idx_salary_list_[id3].insert({user->salary, user->id});
  idx_salary_mtx_list_[id3].unlock();
  return 0;
}

size_t Engine::Read(void *ctx, int32_t select_column,
    int32_t where_column, const void *column_key, 
    size_t column_key_len, void *res) {
  // std::lock_guard<std::mutex> lock(mtx_);
  spdlog::debug("[engine_read] [select_column:{0:d}] [where_column:{1:d}] [column_key_len:{2:d}]", select_column, where_column, column_key_len); 
  User user;
  size_t res_num = 0;
  switch(where_column) {
      case Id: {
        if (column_key_len != 8) {
          spdlog::error("read column_key_len is: {}, expcted: 8", column_key_len);
        }
        int64_t id = *((int64_t *)column_key);
        uint32_t id1 = ((uint32_t)id) % ShardNum;
        idx_id_mtx_list_[id1].lock();
        auto iter = idx_id_list_[id1].find(id);
        if (iter != idx_id_list_[id1].end()) {
          res_num = 1;
          user = iter->second;
          add_res(user, select_column, &res);
        }
        if ((++cnt1_) % 1 == 0) {
          spdlog::debug("[wangqiim] read select_column[{}], where_column[Id], user[{}]", select_column, user.to_string());
        }
        idx_id_mtx_list_[id1].unlock();
      }
      break;

      case Userid: {
        if (column_key_len != 128) {
          spdlog::error("read column_key_len is: {}, expcted: 128", column_key_len);
        }
        uint32_t id2 = (StrHash((char *)column_key, column_key_len)) % ShardNum;

        idx_user_id_mtx_list_[id2].lock();
        auto iter = idx_user_id_list_[id2].find(UserIdWrapper((const char *)column_key));
        if (iter != idx_user_id_list_[id2].end()) {
          res_num = 1;
          int64_t id = iter->second;

          uint32_t id1 = ((uint32_t)id) % ShardNum;
          idx_id_mtx_list_[id1].lock();
          user = idx_id_list_[id1].find(id)->second;
          idx_id_mtx_list_[id1].unlock();
          add_res(user, select_column, &res);
        }
        if ((++cnt2_) % 1 == 0) {
          spdlog::debug("[wangqiim] read select_column[{}], where_column[Userid], user[{}]", select_column, user.to_string());
        }
        idx_user_id_mtx_list_[id2].unlock();
      } 
      break;

      case Name: {
        spdlog::error("don't support select where column[Name]");
      } 
      break;

      case Salary: {
        int64_t salary = *((int64_t *)column_key);
        if ((++cnt4_) % 1 == 0) {
          spdlog::debug("[wangqiim] read select_column[{}], where_column[Salary = {}], writecnt = {}", select_column, salary, write_cnt_);
        }
        if (column_key_len != 8) {
          spdlog::error("read column_key_len is: {}, expcted: 8", column_key_len);
        }

        uint32_t id3 = ((uint32_t)salary) % ShardNum;
        idx_salary_mtx_list_[id3].lock();
        auto range = idx_salary_list_[id3].equal_range(salary);
        auto iter = range.first;
        while (iter != range.second) {
          res_num += 1;
          int64_t id = iter->second;
          uint32_t id1 = ((uint32_t)id) % ShardNum;
          idx_id_mtx_list_[id1].lock();
          user = idx_id_list_[id1].find(id)->second;
          idx_id_mtx_list_[id1].unlock();
          add_res(user, select_column, &res);
          iter++;
        }
        idx_salary_mtx_list_[id3].unlock();
      }
      break;

      default:
        spdlog::error("unexpected where_column: {}", where_column);
      break;
  }
  return res_num;
}

int Engine::replay_index(const std::vector<std::string> paths) {
  Index_Builder index_builder(idx_id_list_, idx_user_id_list_, idx_salary_list_);
  int cnt = 0;
  for (const auto &fname: paths) {
    bool new_create = false;
    if (!Util::FileExists(fname)) {
      int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
      if (fd >= 0) {
        new_create = true;
        spdlog::info("init create log[{}] success!", fname);
        close(fd);
      } else {
        spdlog::error("init create log[{}] fail!", dir_);
        return -1;
      }
    }
    Util::print_file_size(fname);
    if (!new_create) {
      PosixSequentialFile *file = nullptr;
      int retry_num = 0;
      int ret = PosixEnv::NewSequentialFile(fname, &file);
      if (ret == 0) {
        // log is exist, need recovery
        Reader reader(file);
        std::string record;
        while (reader.ReadRecord(record, RecordSize)) {
          const User *user = (const User *)record.data();
          index_builder.build(user);
          cnt++;
        }
        delete(file);
      }
    }
  }
  return cnt;
}
