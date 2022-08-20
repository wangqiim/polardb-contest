#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fstream>
#include <chrono>
#include <thread>

#include "spdlog/spdlog.h"
#include "engine.h"
#include "util.h"
#include "def.h"

thread_local int tid_ = -1;
thread_local int write_cnt = 0;
const std::string phase_name[3] = {"Hybrid", "WriteOnly", "ReadOnly"};

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
      unique_key *idx_user_id, normal_key  *idx_salary, std::vector<User> *users)
      : count_(0), idx_id_(idx_id),
        idx_user_id_(idx_user_id), idx_salary_(idx_salary), users_(users) { }

    // 当is_build为false时，仅仅记录count_，不build索引
    void Scan(const User *user);

    int Get_count() { return count_; }

  private:
    int  count_;
    std::vector<User> *users_;
    primary_key *idx_id_;
    unique_key  *idx_user_id_;
    normal_key  *idx_salary_;
};

void Index_Helper::Scan(const User *user) {

  users_->push_back(*user);

  size_t record_slot = users_->size() - 1;
  // build pk index
  idx_id_->insert({user->id, record_slot});
  // build uk index
  idx_user_id_->emplace(BlizardHashWrapper(user->user_id, UseridLen), record_slot);
  // build nk index
  auto &locations = (*idx_salary_)[user->salary];
  locations.Push(record_slot);
  count_++;
}

void record_scan(const char *record, void *context) {
  const User *user = reinterpret_cast<const User *>(record);
  Index_Helper *helper = reinterpret_cast<Index_Helper *>(context);
  helper->Scan(user);
}

// --------------------Engine-----------------------------
Engine::Engine(const char* aep_dir, const char* disk_dir)
  : is_changing_(false), phase_(Phase::Hybrid), next_tid_(0)
  , mtx_(), aep_dir_(aep_dir), dir_(disk_dir), disk_logs_()
  , pmem_logs_(), idx_id_(), idx_user_id_(), idx_salary_() {
}

Engine::~Engine() {
  auto end = std::chrono::system_clock::now();
  std::chrono::duration<double> elapsed_seconds = end-start_;
  spdlog::info("since init done, elapsed time: {}s", elapsed_seconds.count());

  int record_num = build_3_cluster_index(disk_file_paths_, pmem_file_paths_);
  spdlog::info("there are {} records in db", record_num);

  for (size_t i = 0; i < disk_logs_.size(); i++) {
    delete disk_logs_[i];
  }
  for (size_t i = 0; i < pmem_logs_.size(); i++) {
    delete pmem_logs_[i];
  }
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
  Util::gen_sorted_paths(dir_, WALFileNamePrefix, disk_file_paths_, SSDNum);
  Util::gen_sorted_paths(aep_dir_, WALFileNamePrefix, pmem_file_paths_, AEPNum);
  
  int record_num = replay_index(disk_file_paths_, pmem_file_paths_);
  if (record_num == ClientNum * WritePerClient) {
    is_read_perf_ = true;
    build_3_cluster_index(disk_file_paths_, pmem_file_paths_);
  }
  spdlog::info("init replay build index done, record num = {}", record_num);
  phase_.store(record_num == 0? Phase::WriteOnly: Phase::ReadOnly);

  warmUp();  
  spdlog::info("warmup ssd & pmam done!");

  Util::print_resident_set_size();
  spdlog::info("engine init done, phase_:{}", phase_name[phase_.load()]);
  start_ = std::chrono::system_clock::now();
  return 0;
}

int Engine::Append(const void *datas) {
  if (likely(write_cnt >= 2000)) {
    if (tid_ < AEPNum) {
      pmem_logs_[tid_]->Append(datas);
    } else {
      disk_logs_[tid_ - AEPNum]->Append(datas);
    }
    return 0;
  }
  if (unlikely(phase_.load() == Phase::ReadOnly)) {
    bool current = is_changing_.exchange(true);
    if (current == false) {
      // current可能和一个刚刚建完索引的线程冲突，因此做一次double check
      if (phase_.load() == Phase::ReadOnly) {
        // 1. 睡眠一段时间，保证其他线程探测到is_changing_ = true 并且阻塞住，
        // 使得建立索引的过程中没有正在进行中的R/W（只能说无锁，尽量多睡眠一段时间）
        sleep(FenceSecond);
        // 2. 先修改phase_
        phase_.store(Phase::Hybrid);
        // 3. 再修改is_changing_
        is_changing_.store(false);
        spdlog::info("phase change: ReadOnly -> Hybrid");
      }
    }
  }
  while (unlikely(is_changing_.load() == true)) {
    sleep(WaitChangeFinishSecond);
  }
  must_set_tid();
  int cur_phase = phase_.load();
  if (cur_phase == Phase::Hybrid) {
    mtx_.lock();
  }
  const User *user = reinterpret_cast<const User *>(datas);

  if (tid_ < AEPNum) {
    pmem_logs_[tid_]->Append(datas);
  } else {
    disk_logs_[tid_ - AEPNum]->Append(datas);
  }

  if (cur_phase == Phase::Hybrid) {
    users_.push_back(*user);
    size_t record_slot = users_.size() - 1;
    idx_id_.insert({user->id, record_slot});
    // build uk index
    idx_user_id_.emplace(BlizardHashWrapper(user->user_id, UseridLen), record_slot); // avoid unneccessary copy constructer

    // build nk index
    
    LocationsWrapper &locations = idx_salary_[user->salary];
    locations.Push(record_slot);
  }
  
  if (cur_phase == Phase::Hybrid) {
    mtx_.unlock();
  }
  write_cnt++;
  if (write_cnt == WritePerClient) {
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed_seconds = end-start_;
    spdlog::info("tid[{}] finish write {} records, elapsed time: {}s", tid_, WritePerClient, elapsed_seconds.count());
  }
  return 0;
}

size_t Engine::Read(void *ctx, int32_t select_column,
    int32_t where_column, const void *column_key, 
    size_t column_key_len, void *res) {
  if (likely(is_read_perf_)) {
    return perf_Read(ctx, select_column, where_column, column_key, column_key_len, res);
  }
  if (phase_.load() == Phase::WriteOnly) {
    bool current = is_changing_.exchange(true);
    if (current == false) {
      // current可能和一个刚刚建完索引的线程冲突，因此做一次double check
      if (phase_.load() == Phase::WriteOnly) {
        // 由本线程负责建索引
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
        int64_t id = *((int64_t *)column_key);
        auto iter = idx_id_.find(id);
        if (iter != idx_id_.end()) {
          res_num = 1;
          add_res(users_[iter->second], select_column, &res);
        }
      }
      break;

      case Userid: {
        auto iter = idx_user_id_.find(BlizardHashWrapper(reinterpret_cast<const char*>(column_key), UseridLen));
        if (iter != idx_user_id_.end()) {
          res_num = 1;
          add_res(users_[iter->second], select_column, &res);
        }
      } 
      break;

      case Name: {
        spdlog::error("don't support select where column[Name]");
      } 
      break;

      case Salary: {
        int64_t salary = *((int64_t *)column_key);
        auto iter = idx_salary_.find(salary);
        if (iter != idx_salary_.end()) {
          for (size_t i = 0; i < iter->second.Size(); i++) {
            res_num += 1;
            add_res(users_[iter->second[i]], select_column, &res);
          }
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
  users_.clear();
  idx_id_.reserve(WritePerClient * ClientNum);
  idx_user_id_.reserve(WritePerClient * ClientNum);
  idx_salary_.reserve(WritePerClient * ClientNum);
  users_.reserve(WritePerClient * ClientNum);
  Index_Helper index_builder(&idx_id_, &idx_user_id_, &idx_salary_, &users_);
  for (size_t log_id = 0; log_id < disk_path.size(); log_id++) {
    // 如果ret != 0,没有给file分配内存,因此可以让reader管理file指针的内存,reader离开作用域时，会调用reader的析构函数释放file指针的空间
    MmapReader reader(disk_path[log_id], MmapSize);
    char *record;
    while (reader.ReadRecord(record, RecordSize)) {
      const User *user = (const User *)record;
      index_builder.Scan(user);
    }
  }
  for (size_t log_id = 0; log_id < pmem_path.size(); log_id++) {
    PmapBufferReader reader(pmem_path[log_id], PoolSize);
    char *record;
    while (reader.ReadRecord(record, RecordSize)) {
      const User *user = (const User *)record;
      index_builder.Scan(user);
    }
  }
  open_all_writers();
  spdlog::info("replay index done, record num = {}", index_builder.Get_count());
  return index_builder.Get_count();
}

inline int Engine::must_set_tid() {
  if (unlikely(tid_ == -1)) {
    tid_ = next_tid_.fetch_add(1);
    if (tid_ >= ClientNum) {
      spdlog::warn("w/r thread excceed 50!!");
      tid_ %= ClientNum;
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
  for (const auto &fname: disk_file_paths_) {
    disk_logs_.push_back(new MmapWriter(fname, MmapSize));
  }
  for (const auto &fname: pmem_file_paths_) {
    pmem_logs_.push_back(new PmapBufferWriter(fname, PoolSize));
  }
  return 0;
}

// read formance
// ------Index Builder-------------
class Cluster_Index_Helper {
  public:
    Cluster_Index_Helper(cluster_primary_key *cluster_idx_id,
      cluster_unique_key *cluster_idx_user_id, 
      cluster_normal_key  *cluster_idx_salary)
      : count_(0), cluster_idx_id_(cluster_idx_id),
        cluster_idx_user_id_(cluster_idx_user_id), 
        cluster_idx_salary_(cluster_idx_salary){ }

    // 当is_build为false时，仅仅记录count_，不build索引
    void Scan(const User *user);

    int Get_count() { return count_; }

  private:
    int  count_;
    cluster_primary_key *cluster_idx_id_;
    cluster_unique_key  *cluster_idx_user_id_;
    cluster_normal_key  *cluster_idx_salary_;
};

void Cluster_Index_Helper::Scan(const User *user) {
  // build pk index
  cluster_idx_id_->emplace(user->id, user->user_id);
  // build uk index
  cluster_idx_user_id_->emplace(BlizardHashWrapper(user->user_id, UseridLen), user->name);
  // build nk index
  cluster_idx_salary_->emplace(user->salary, user->id);
  count_++;
}

int Engine::build_3_cluster_index(const std::vector<std::string> disk_path, const std::vector<std::string> pmem_path) {
  // 我不确定对于同一个文件或pmem同时读写打开会不会有问题，因此在这里重新关闭之后再次打开了writers。
  close_all_writers();
  // clear() is not work here for release memory in c++
  idx_id_ = primary_key();
  idx_user_id_ = unique_key();
  idx_salary_ = normal_key();
  users_ = std::vector<User>();
  cluster_idx_id_ = cluster_primary_key();
  cluster_idx_user_id_ = cluster_unique_key();
  cluster_idx_salary_ = cluster_normal_key();
  cluster_idx_id_.reserve(WritePerClient * ClientNum);
  cluster_idx_user_id_.reserve(WritePerClient * ClientNum);
  cluster_idx_salary_.reserve(WritePerClient * ClientNum);
  Cluster_Index_Helper index_builder(&cluster_idx_id_, &cluster_idx_user_id_, &cluster_idx_salary_);
  for (size_t log_id = 0; log_id < disk_path.size(); log_id++) {
    // 如果ret != 0,没有给file分配内存,因此可以让reader管理file指针的内存,reader离开作用域时，会调用reader的析构函数释放file指针的空间
    MmapReader reader(disk_path[log_id], MmapSize);
    char *record;
    while (reader.ReadRecord(record, RecordSize)) {
      const User *user = (const User *)record;
      index_builder.Scan(user);
    }
  }
  for (size_t log_id = 0; log_id < pmem_path.size(); log_id++) {
    PmapBufferReader reader(pmem_path[log_id], PoolSize);
    char *record;
    while (reader.ReadRecord(record, RecordSize)) {
      const User *user = (const User *)record;
      index_builder.Scan(user);
    }
  }
  open_all_writers();
  spdlog::info("build_3_cluster_index done, record num = {}", index_builder.Get_count());
  return index_builder.Get_count();
}

inline size_t Engine::perf_Read(void *ctx, int32_t select_column,
    int32_t where_column, const void *column_key, 
    size_t column_key_len, void *res) {
  size_t res_num = 0;
  switch(where_column) {
      case Id: {
        int64_t id = *((int64_t *)column_key);
        auto iter = cluster_idx_id_.find(id);
        if (iter != cluster_idx_id_.end()) {
          res_num = 1;
          memcpy(res, iter->second.s, 128); 
        }
      }
      break;

      case Userid: {
        auto iter = cluster_idx_user_id_.find(BlizardHashWrapper(reinterpret_cast<const char*>(column_key), UseridLen));
        if (iter != cluster_idx_user_id_.end()) {
          res_num = 1;
          memcpy(res, iter->second.s, 128); 
        }
      } 
      break;

      case Name: {
        spdlog::error("don't support select where column[Name]");
      } 
      break;

      case Salary: {
        int64_t salary = *((int64_t *)column_key);
        auto iter = cluster_idx_salary_.find(salary);
        if (iter != cluster_idx_salary_.end()) {
          res_num = 1;
          memcpy(res, &iter->second, 8); 
        }
      }
      break;

      default:
        spdlog::error("unexpected where_column: {}", where_column);
      break;
  }
  return res_num;
}

// 对于mmap，有两种最直接的warmup思路。假设pagecache大小能容纳6个slot
// 方案1:
// writer1: slot(w) slot(w) slot(nw) slot(nw)
// writer2: slot(w) slot(w) slot(nw) slot(nw)
// writer3: slot(w) slot(w) slot(nw) slot(nw)
// 方案2:
// writer2: slot(w) slot(w) slot(w) slot(w)
// writer3: slot(w) slot(w) slot(nw) slot(nw)
// writer3: slot(nw) slot(nw) slot(nw) slot(nw)
// 在这里我采用方案1
void Engine::warmUp() {
  if (disk_logs_.size() > 0) {
    // 注意：假设每个mmapwriter的大小是一样的
    size_t max_slot = disk_logs_[0]->MaxSlot();
    // 从后往前warmup，那么理论上来说先被置换出去的页应该就是尾页
    // 这样当发生驱逐时，会先驱逐出mmap的地址空间后面一部分pagecache，应该会好一点
    for (size_t i = max_slot; i != 0; i--) {
      for (const auto &mmapWriter: disk_logs_) {
        mmapWriter->WarmUp(i - 1);
      }
    }
  }
  for (const auto &pmemWriter: pmem_logs_) {
    // pmemwriter的buffer很小，直接warmup整个buffer
    pmemWriter->WarmUp();
  }
}
