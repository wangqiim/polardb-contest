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
const int WaitChangeFinishSecond = 3;
const int FenceSecond = 10;

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
class Index_Helper {
  public:
    Index_Helper(primary_key *idx_id,
      unique_key *idx_user_id, normal_key  *idx_salary)
      : count_(0), idx_id_(idx_id),
        idx_user_id_(idx_user_id), idx_salary_(idx_salary) { }

    // 当is_build为false时，仅仅记录count_，不build索引
    void Scan(const User *user);

    int Get_count() { return count_; }

  private:
    int  count_;

    primary_key *idx_id_;
    unique_key  *idx_user_id_;
    normal_key  *idx_salary_;
};

void Index_Helper::Scan(const User *user) {
  // build pk index
  idx_id_->insert({user->id, *user});
  // build uk index
  idx_user_id_->insert({UserIdWrapper(user->user_id), user->id});
  // build nk index
  idx_salary_->insert({user->salary,user->id});
  count_++;
}

void record_scan(const char *record, void *context) {
  const User *user = reinterpret_cast<const User *>(record);
  Index_Helper *helper = reinterpret_cast<Index_Helper *>(context);
  helper->Scan(user);
}

// --------------------Engine-----------------------------
Engine::Engine(const char* aep_dir, const char* disk_dir)
  : next_tid_(0), mtx_(), aep_dir_(aep_dir), dir_(disk_dir), disk_logs_(), pmem_logs_(),
    idx_id_(), idx_user_id_(), idx_salary_(), phase_(Phase::Hybrid) {}

Engine::~Engine() {
  for (size_t i = 0; i < disk_logs_.size(); i++) {
    delete disk_logs_[i];
  }
  for (size_t i = 0; i < pmem_logs_.size(); i++) {
    delete pmem_logs_[i];
  }
  auto end = std::chrono::system_clock::now();
  std::chrono::duration<double> elapsed_seconds = end-start_;
  spdlog::info("since init done, elapsed time: {}s", elapsed_seconds.count());
}

int Engine::Init() {
  if (!Util::FileExists(aep_dir_)) {
    spdlog::info("aep_dir_: {} path is not exist, start to create it", aep_dir_);
    if (0 != mkdir(aep_dir_.c_str(), 0755)) {
      spdlog::error("init create aep_dir_[{}] fail!", aep_dir_);
    }
  }
  if (!Util::FileExists(dir_)) {
    spdlog::info("dir_: {} path is not exist, start to create it", dir_);
    if (0 != mkdir(dir_.c_str(), 0755)) {
      spdlog::error("init create dir[{}] fail!", dir_);
    }
  }

  // build index
  Util::gen_sorted_paths(dir_, kWALFileName, disk_file_paths_, SSDNum);
  Util::gen_sorted_paths(aep_dir_, kWALFileName, pmem_file_paths_, AEPNum);
  
  int record_num = replay_index(disk_file_paths_, pmem_file_paths_);
  spdlog::info("init replay build index done, record num = {}", record_num);
  phase_.store(record_num == 0? Phase::WriteOnly: Phase::ReadOnly);

  Util::print_resident_set_size();
  spdlog::info("engine init done, phase_:{}", phase_.load());
  start_ = std::chrono::system_clock::now();
  return 0;
}

int Engine::Append(const void *datas) {
  if (phase_.load() == Phase::ReadOnly) {
    bool current = is_changing_.exchange(true);
    if (current == false) {
      // current可能和一个刚刚建完索引的线程冲突，因此做一次double check
      if (phase_.load() == Phase::ReadOnly) {
        // 由本线程负责建索引
        // todo(wq):
        // 1. 睡眠一段时间，保证其他线程探测到is_changing_ = true 并且阻塞住，
        // 使得建立索引的过程中没有正在进行中的R/W（只能说无锁，尽量多睡眠一段时间）
        sleep(FenceSecond);
        // 2. 开始建立索引
        replay_index(disk_file_paths_, pmem_file_paths_);
        // 3. 先修改phase_
        phase_.store(Phase::Hybrid);
        // 4. 再修改is_changing_
        is_changing_.store(false);
        spdlog::info("phase change: ReadOnly -> Hybrid");
      }
    } else {
      // 等到状态变更完成
      while (is_changing_.load() == true) {
        sleep(WaitChangeFinishSecond);
      }
    }
  }
  while (is_changing_.load() == true) {
    sleep(WaitChangeFinishSecond);
  }
  must_set_tid();
  int cur_phase = phase_.load();
  if (cur_phase == Phase::Hybrid) {
    mtx_.lock();
  }
  const User *user = reinterpret_cast<const User *>(datas);

  if (tid_ < SSDNum) {
    disk_logs_[tid_]->AddRecord(datas, RecordSize);
  } else {
    pmem_logs_[tid_ - SSDNum]->Append(datas, RecordSize);
  }

  if (cur_phase == Phase::Hybrid) {
    idx_id_.insert({user->id, *user});
    // build uk index
    idx_user_id_.emplace(user->user_id, user->id); // avoid unneccessary copy constructer

    // build nk index
    idx_salary_.insert({user->salary, user->id});
  }
  
  if (cur_phase == Phase::Hybrid) {
    mtx_.unlock();
  }
  return 0;
}

size_t Engine::Read(void *ctx, int32_t select_column,
    int32_t where_column, const void *column_key, 
    size_t column_key_len, void *res) {
  if (phase_.load() == Phase::WriteOnly) {
    bool current = is_changing_.exchange(true);
    if (current == false) {
      // current可能和一个刚刚建完索引的线程冲突，因此做一次double check
      if (phase_.load() == Phase::WriteOnly) {
        // 由本线程负责建索引
        // todo(wq):
        // 1. 睡眠一段时间，保证其他线程探测到is_changing_ = true 并且阻塞住，
        // 使得建立索引的过程中没有正在进行中的R/W（只能说无锁，尽量多睡眠一段时间）
        sleep(FenceSecond);
        // 2. 开始建立索引
        replay_index(disk_file_paths_, pmem_file_paths_);
        // 3. 先修改phase_
        phase_.store(Phase::Hybrid);
        // 4. 再修改is_changing_
        is_changing_.store(false);
        spdlog::info("phase change: WriteOnly -> Hybrid");
      }
    } else {
      // 等到状态变更完成
      while (is_changing_.load() == true) {
        sleep(WaitChangeFinishSecond);
      }
    }
  }
  while (is_changing_.load() == true) {
    sleep(WaitChangeFinishSecond);
  }
  must_set_tid();
  int cur_phase = phase_.load();
  if (cur_phase == Phase::Hybrid) {
    mtx_.lock();
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
        auto iter = idx_id_.find(id);
        if (iter != idx_id_.end()) {
          res_num = 1;
          add_res(iter->second, select_column, &res);
        }
      }
      break;

      case Userid: {
        if (column_key_len != 128) {
          spdlog::error("read column_key_len is: {}, expcted: 128", column_key_len);
        }
        auto iter = idx_user_id_.find(UserIdWrapper((const char *)column_key));
        if (iter != idx_user_id_.end()) {
          int64_t id = iter->second;
          auto iter_id = idx_id_.find(id);
          if (iter_id != idx_id_.end()) {
            res_num = 1;
            add_res(iter_id->second, select_column, &res);
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
        auto range = idx_salary_.equal_range(salary);
        auto iter = range.first;
        while (iter != range.second) {
          int64_t id = iter->second;
          auto iter_id = idx_id_.find(id);
          if (iter_id != idx_id_.end()) {
            res_num += 1;
            add_res(iter_id->second, select_column, &res);
          }
          iter++;
        }
      }
      break;

      default:
        spdlog::error("unexpected where_column: {}", where_column);
      break;
  }
  if (cur_phase == Phase::Hybrid) {
    mtx_.unlock();
  }
  return res_num;
}

int Engine::replay_index(const std::vector<std::string> disk_path, const std::vector<std::string> pmem_path) {
  // 我不确定对于同一个文件或pmem同时读写打开会不会有问题，因此在这里重新关闭之后再次打开了writers。
  close_all_writers();
  idx_id_.clear();
  idx_user_id_.clear();
  idx_salary_.clear();
  Index_Helper index_builder(&idx_id_, &idx_user_id_, &idx_salary_);
  for (size_t log_id = 0; log_id < disk_path.size(); log_id++) {
    Util::CreateIfNotExists(disk_path[log_id]);
    PosixSequentialFile *file = nullptr;
    int ret = PosixEnv::NewSequentialFile(disk_path[log_id], &file);
    if (ret == 0) {
      // 如果ret != 0,没有给file分配内存,因此可以让reader管理file指针的内存,reader离开作用域时，会调用reader的析构函数释放file指针的空间
      Reader reader(file);
      std::string record;
      while (reader.ReadRecord(record, RecordSize)) {
        const User *user = (const User *)record.data();
        index_builder.Scan(user);
      }
    }
  }
  for (size_t log_id = 0; log_id < pmem_path.size(); log_id++) {
    PmemReader reader = PmemReader(pmem_path[log_id], PoolSize);
    reader.Scan(record_scan, &index_builder);
  }
  open_all_writers();
  spdlog::info("replay index done, record num = {}", index_builder.Get_count());
  return index_builder.Get_count();
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

void Engine::close_all_writers() {
for (size_t i = 0; i < disk_logs_.size(); i++) {
    delete disk_logs_[i];
  }
  for (size_t i = 0; i < pmem_logs_.size(); i++) {
    delete pmem_logs_[i];
  }
  disk_logs_.clear();
  pmem_logs_.clear();
}

int Engine::open_all_writers() {
  PosixWritableFile *walfile = nullptr;
  for (const auto &fname: disk_file_paths_) {
    int ret = PosixEnv::NewAppendableFile(fname, &walfile);
    if (ret != 0) {
      return -1;
    }
    disk_logs_.push_back(new Writer(walfile));
  }
  for (const auto &fname: pmem_file_paths_) {
    pmem_logs_.push_back(new PmemWriter(fname, PoolSize));
  }
  return 0;
}
