#include "engine.h"

Engine::Engine(const char* disk_dir)
  : mtx_(), dir_(disk_dir), plate_(new Plate(dir_)),
    idx_id_(), idx_user_id_(), idx_salary_() {}

Engine::~Engine() {
  delete plate_;
}