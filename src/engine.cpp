#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fstream>

#include "spdlog/spdlog.h"
#include "engine.h"

void print_resident_set_size() {
  double resident_set = 0.0;
  std::ifstream stat_stream("/proc/self/stat",std::ios_base::in);
  std::string pid, comm, state, ppid, pgrp, session, tty_nr;
  std::string tpgid, flags, minflt, cminflt, majflt, cmajflt;
  std::string utime, stime, cutime, cstime, priority, nice;
  std::string O, itrealvalue, starttime;
  unsigned long vsize;
  long rss;
  stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr
          >> tpgid >> flags >> minflt >> cminflt >> majflt >> cmajflt
          >> utime >> stime >> cutime >> cstime >> priority >> nice
          >> O >> itrealvalue >> starttime >> vsize >> rss;
  stat_stream.close();
  long page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024;
  resident_set = (double)rss * (double)page_size_kb / (1024 * 1024);
  spdlog::info("plate current process memory size: {0:f}g", resident_set);
}

bool FileExists(const std::string& path) {
  return access(path.c_str(), F_OK) == 0;
}

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
  idx_user_id_[hid % ShardNum].insert({std::string(user->user_id, sizeof(user->user_id)), user->id});
  // build nk index
  idx_salary_[((uint32_t)user->salary) % ShardNum].insert({user->salary,user->id});
}

// --------------------Engine-----------------------------
Engine::Engine(const char* disk_dir)
  : mtx_(), dir_(disk_dir), log_(nullptr),
    idx_id_list_(), idx_user_id_list_(), idx_salary_list_() {}

Engine::~Engine() {
  if (log_ == nullptr) {
    spdlog::error("log_ is nullptr before ~Engine()");
  }
  delete (log_->GetFile());
  delete log_;
  log_ = nullptr;
}

int Engine::Init() {
  std::lock_guard<std::mutex> lock(mtx_);

  Index_Builder index_builder(idx_id_list_, idx_user_id_list_, idx_salary_list_);
  
  spdlog::info("engine start init");

  // create dir
  if (!FileExists(dir_) && 0 != mkdir(dir_.c_str(), 0755)) {
    spdlog::error("init create dir[{}] fail!", dir_);
    return -1;
  }

  std::string fname = WALFileName(dir_);
  // create log
  if (!FileExists(fname)) {
    int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd >= 0) {
      close(fd);
    } else {
      spdlog::error("init create log[{}] fail!", dir_);
      return -1;
    }
  }

  PosixSequentialFile *file = nullptr;
  int retry_num = 0;
  int ret = 0;
  while ((ret = PosixEnv::NewSequentialFile(fname, &file)) != 0) {
    spdlog::info("open NewSequentialFile[{}] fail, retry num: {}, sleep 1s", fname, retry_num);
    sleep(1); // sleep 1 second
    if (retry_num == 3) {
      break;
    }
    retry_num++;
  }
  if (ret == 0) {
    int cnt = 0;
    // log is exist, need recovery
    Reader reader(file);
    std::string record;
    while (reader.ReadRecord(record, RecordSize)) {
      const User *user = (const User *)record.data();
      index_builder.build(user);
      cnt++;
    }
    spdlog::info("build index done, num = {}", cnt);
    delete(file);
  }
  
  PosixWritableFile *walfile = nullptr;
  ret = PosixEnv::NewAppendableFile(fname, &walfile);
  if (ret != 0) {
    return -1;
  }
  log_ = new Writer(walfile);
  print_resident_set_size();
  spdlog::info("engine init done");
  return 0;
}

int Engine::Append(const void *datas) {
  if ((++write_cnt_) % 1 == 0) {
    spdlog::debug("[wangqiim] write {}", ((User *)datas)->to_string());
  }
  mtx_.lock();
  log_->AddRecord(datas, RecordSize);
  mtx_.unlock();
  const User *user = reinterpret_cast<const User *>(datas);
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
  idx_user_id_list_[id2].insert({std::string(user->user_id, sizeof(user->user_id)), user->id}); // must use string(char* s, size_t n) construct funciton
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
        std::string user_id((char *)column_key, column_key_len); // todo: column_key_len is 128???
        uint32_t id2 = (StrHash((char *)column_key, column_key_len)) % ShardNum;

        idx_user_id_mtx_list_[id2].lock();
        auto iter = idx_user_id_list_[id2].find(user_id);
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