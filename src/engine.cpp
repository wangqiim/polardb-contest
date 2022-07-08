#include "engine.h"
#include "user.h"
#include "spdlog/spdlog.h"

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

    void build(User *user, Location *location);

    primary_key *idx_id_;
    unique_key  *idx_user_id_;
    normal_key  *idx_salary_; 
};

void Index_Builder::build(User *user, Location *location) {
  // build pk index
  idx_id_->insert({user->id, Location(location->file_id_, location->offset_)});
  // build uk index
  idx_user_id_->insert({std::string(user->user_id, sizeof(user->user_id)), Location(location->file_id_, location->offset_)});
  // build nk index
  idx_salary_->insert({user->salary, Location(location->file_id_, location->offset_)});
}

void build_index(void *record, void *location, void *context) {
    User *user = reinterpret_cast<User *>(record);
    Location *loc = reinterpret_cast<Location *>(location);
    Index_Builder *builder = reinterpret_cast<Index_Builder *>(context);
    builder->build(user, loc);
}

// ---------Name sequence scan------------------------
class EngineReader { // we should only use it when where_column isn't index
  public:
      EngineReader(int32_t select_column, int32_t where_column, const void *column_key, size_t column_key_len, void *res)
        : select_column_(select_column), 
        where_column_(where_column), 
        column_key_(column_key), 
        column_key_len_(column_key_len),
        res_(res),
        res_num_(0) {
          if (where_column != Name) {
            spdlog::error("we should only use it when where_column is name:{}, but it is {}", Name, where_column);
          }
        }
      
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
  if (b) {
    ++res_num_;
    add_res(*user, select_column_, &res_);
  }
}

void read_record(void *record, void *location, void *context) {
    User *user = reinterpret_cast<User *>(record);
    EngineReader *reader = reinterpret_cast<EngineReader *>(context);
    reader->read(user);
}

// --------------------Engine-----------------------------
Engine::Engine(const char* disk_dir)
  : mtx_(), dir_(disk_dir), plate_(new Plate(dir_)),
    idx_id_(), idx_user_id_(), idx_salary_() {}

Engine::~Engine() {
  delete plate_;
}

int Engine::Init() {
  std::lock_guard<std::mutex> lock(mtx_);
  spdlog::info("engine start init");
  int ret = plate_->Init();
  if (ret != 0) {
    spdlog::error("plate init fail");
    return -1;
  }
  Index_Builder index_builder(&idx_id_, &idx_user_id_, &idx_salary_);
  ret = plate_->scan(build_index, &index_builder);
  if (ret != 0) {
    spdlog::error("plate scan fail");
    return -1;
  }
  spdlog::info("engine init done");
  return 0;
}

int Engine::Append(const void *datas) {
  std::lock_guard<std::mutex> lock(mtx_);
  Location location;
  plate_->append(datas, location);
  const User *user = reinterpret_cast<const User *>(datas);
  // build pk index
  idx_id_.insert({user->id, location});
  // build uk index
  idx_user_id_.insert({std::string(user->user_id, sizeof(user->user_id)), location}); // must use string(char* s, size_t n) construct funciton
  // build nk index
  idx_salary_.insert({user->salary, location});
  return 0;
}

size_t Engine::Read(void *ctx, int32_t select_column,
    int32_t where_column, const void *column_key, 
    size_t column_key_len, void *res) {
  std::lock_guard<std::mutex> lock(mtx_);
  User user;
  size_t res_num = 0;
  switch(where_column) {
      case Id: {
        int64_t id = *((int64_t *)column_key);
        auto iter = idx_id_.find(id);
        if (iter != idx_id_.end()) {
          res_num = 1;
          plate_->get(iter->second, &user);
          add_res(user, select_column, &res);
        }
      }
      break;

      case Userid: {
        std::string user_id((char *)column_key, column_key_len);
        auto iter = idx_user_id_.find(user_id);
        if (iter != idx_user_id_.end()) {
          res_num = 1;
          plate_->get(iter->second, res);
          add_res(user, select_column, &res);
        }
      } 
      break;

      case Name: {
        EngineReader reader(select_column, where_column, column_key, column_key_len, res);
        plate_->scan(read_record, reinterpret_cast<void *>(&reader));
        res_num = reader.get_cnt();
      } 
      break;

      case Salary: {
        int64_t salary = *((int64_t *)column_key);
        auto range = idx_salary_.equal_range(salary);
        auto iter = range.first;
        while (iter != range.second) {
          res_num += 1;
          plate_->get(iter->second, &user);
          add_res(user, select_column, &res);
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