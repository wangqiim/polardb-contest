#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fstream>

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
  , mtx_(), log_buffer_mgr_(nullptr), aep_dir_(aep_dir), ssd_dir_(disk_dir), log_writers_()
  , idx_id_(), idx_user_id_(), idx_salary_() {
}

Engine::~Engine() {
  auto end = std::chrono::system_clock::now();
  std::chrono::duration<double> elapsed_seconds = end-start_;
  spdlog::info("since init done, elapsed time: {}s", elapsed_seconds.count());
  for (size_t i = 0; i < log_writers_.size(); i++) {
    delete log_writers_[i];
  }
  delete log_buffer_mgr_;
}

int Engine::Init() {
  if (!Util::FileExists(aep_dir_)) {
    spdlog::info("aep_dir_: {} path is not exist, start to create it", aep_dir_);
    if (0 != mkdir(aep_dir_.c_str(), 0755)) {
      spdlog::error("init create aep_dir_[{}] fail!", aep_dir_);
    }
  }
  if (!Util::FileExists(ssd_dir_)) {
    spdlog::info("dir_: {} path is not exist, start to create it", ssd_dir_);
    if (0 != mkdir(ssd_dir_.c_str(), 0755)) {
      spdlog::error("init create dir[{}] fail!", ssd_dir_);
    }
  }

  // build index
  ssd_file_path_ = ssd_dir_+ "/" + WALFileNameSuffix;
  pmem_file_path_ = aep_dir_+ "/" + WALFileNameSuffix;

  log_buffer_mgr_ = new LogBufferMgr(ssd_file_path_.c_str(), pmem_file_path_.c_str());
  log_buffer_mgr_->StartFlushRun();
  open_all_writers();
  
  int record_num = replay_index(ssd_file_path_, pmem_file_path_);
  if (record_num == ClientNum * WritePerClient) {
    is_read_perf_ = true;
    build_3_cluster_index(ssd_file_path_, pmem_file_path_);
  }
  spdlog::info("init replay build index done, record num = {}", record_num);
  phase_.store(record_num == 0? Phase::WriteOnly: Phase::ReadOnly);

  spdlog::info("warmup ssd & pmam done!");

  Util::print_resident_set_size();
  spdlog::info("engine init done, phase_:{}", phase_name[phase_.load()]);
  start_ = std::chrono::system_clock::now();
  return 0;
}

int Engine::Append(const void *datas) {
  if (phase_.load() == Phase::ReadOnly) {
    phase_.store(Phase::Hybrid);
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

  log_writers_[tid_]->Append(datas);

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
  // write_cnt++;
  // if (write_cnt == WritePerClient) {
  //   auto end = std::chrono::system_clock::now();
  //   std::chrono::duration<double> elapsed_seconds = end-start_;
  //   spdlog::info("tid[{}] finish write {} records, elapsed time: {}s", tid_, WritePerClient, elapsed_seconds.count());
  // }
  return 0;
}

size_t Engine::Read(void *ctx, int32_t select_column,
    int32_t where_column, const void *column_key, 
    size_t column_key_len, void *res) {
  if (is_read_perf_) {
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
        replay_index(ssd_file_path_, pmem_file_path_);
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

int Engine::replay_index(const std::string &disk_path, const std::string &pmem_path) {
  // 我不确定对于同一个文件或pmem同时读写打开会不会有问题，因此在这里重新关闭之后再次打开了writers。
  idx_id_.clear();
  idx_user_id_.clear();
  idx_salary_.clear();
  users_.clear();
  idx_id_.reserve(WritePerClient * ClientNum);
  idx_user_id_.reserve(WritePerClient * ClientNum);
  idx_salary_.reserve(WritePerClient * ClientNum);
  users_.reserve(WritePerClient * ClientNum);
  Index_Helper index_builder(&idx_id_, &idx_user_id_, &idx_salary_, &users_);
  // 如果ret != 0,没有给file分配内存,因此可以让reader管理file指针的内存,reader离开作用域时，会调用reader的析构函数释放file指针的空间
  LogReader reader(log_buffer_mgr_);
  char *record;
  while (reader.ReadRecord(record, RecordSize) == 0) {
    const User *user = (const User *)record;
    index_builder.Scan(user);
  }
  spdlog::info("replay index done, record num = {}", index_builder.Get_count());
  return index_builder.Get_count();
}

int Engine::open_all_writers() {
  if (log_writers_.size() != 0) {
    spdlog::error("log_writers_ is not empty, when open writers");
  }
  for (int i = 0; i < ClientNum; i++) {
    log_writers_.push_back(new LogWriter(log_buffer_mgr_));
  }
  return 0;
}

inline int Engine::must_set_tid() {
  if (tid_ == -1) {
    tid_ = next_tid_.fetch_add(1);
    if (tid_ >= ClientNum) {
      spdlog::warn("w/r thread excceed 50!!");
      tid_ %= ClientNum;
    }
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

int Engine::build_3_cluster_index(const std::string &disk_path, const std::string &pmem_path) {
  // clear() is not work here for release memory in c++
  idx_id_ = primary_key();
  idx_user_id_ = unique_key();
  idx_salary_ = normal_key();
  users_ = std::vector<User>();
  cluster_idx_id_.reserve(WritePerClient * ClientNum);
  cluster_idx_user_id_.reserve(WritePerClient * ClientNum);
  cluster_idx_salary_.reserve(WritePerClient * ClientNum);
  Cluster_Index_Helper index_builder(&cluster_idx_id_, &cluster_idx_user_id_, &cluster_idx_salary_);
  // 如果ret != 0,没有给file分配内存,因此可以让reader管理file指针的内存,reader离开作用域时，会调用reader的析构函数释放file指针的空间
  LogReader reader(log_buffer_mgr_);
  char *record;
  while (reader.ReadRecord(record, RecordSize) == 0) {
    const User *user = (const User *)record;
    index_builder.Scan(user);
  }
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
