#include <vector>
#include <string>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fstream>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include "spdlog/spdlog.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

class Util {
  public:
    static std::string DataFileName(const std::string &dir, const std::string &fileName, int number) {
      std::string dataFileName = dir + "/" + fileName + "_";
      char buf[100] = {0};
      std::snprintf(buf, sizeof(buf), "%08d", number);
      return dataFileName + buf;
    }

    static void gen_sorted_paths(const std::string &dir, const std::string &fileName, std::vector<std::string> &paths, int file_num) {
      if (paths.size() != 0) {
        spdlog::error("call gen_sorted_paths when paths is not empty!");
        return;
      }
      for (int i = 0; i < file_num; i++) {
        paths.push_back(DataFileName(dir, fileName, i));
      }
    }

    static void print_resident_set_size() {
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

    static void print_file_size(const std::string &fname) {
      struct stat st;
      int ret = stat(fname.c_str(), &st);
      if (ret != 0) {
        spdlog::error("can't print file size: {}", fname);
        return;
      }
      off_t sz = st.st_size;
      double file_size = double(sz) / (1024 * 1024 * 1024);
      spdlog::info("{0}, size: {1:f}g", fname, file_size);
    }

    static bool FileExists(const std::string& path) {
      return access(path.c_str(), F_OK) == 0;
    }

    static int evaluate_files_record_nums(std::vector<std::string> &paths, int record_size) {
      int num = 0;
      for (const auto &fname: paths) {
        if (!FileExists(fname)) {
          continue;
        }
        struct stat st;
        int ret = stat(fname.c_str(), &st);
        if (ret != 0) {
          spdlog::error("can't print file size: {}", fname);
          return -1;
        }
        off_t sz = st.st_size;
        num += int(sz) / record_size;
      }
      return num;
    }
};
