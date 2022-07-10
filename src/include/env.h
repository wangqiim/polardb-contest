#include <string>

constexpr const size_t kWritableFileBufferSize = 65536;
constexpr const int kBlockSize = 32768;
constexpr const int RecordSize = 8 + 128 + 128 + 8;
int PosixError(int error_number);

class PosixSequentialFile {
 public:
  PosixSequentialFile(std::string filename, int fd);
  ~PosixSequentialFile();

  int Read(size_t n, std::string &result, char* scratch);

  int Skip(uint64_t n);
 private:
  const int fd_;
  const std::string filename_;
};

class PosixWritableFile {
 public:
  static int SyncFd(int fd, const std::string& fd_path);

  PosixWritableFile() = default;
  PosixWritableFile(std::string filename, int fd);
  
  ~PosixWritableFile();

  PosixWritableFile(const PosixWritableFile&) = delete;
  PosixWritableFile& operator=(const PosixWritableFile&) = delete;

  int Append(const void* data, const size_t len);
  int Close();
  int Flush();
  int Sync();
 private:
 int FlushBuffer();
 int WriteUnbuffered(const char* data, size_t size);

  char buf_[kWritableFileBufferSize];
  size_t pos_;
  int fd_;

  const std::string filename_;
};

class PosixEnv {
  public:
    static int NewSequentialFile(const std::string& filename, PosixSequentialFile** result);
    static int NewAppendableFile(const std::string& filename, PosixWritableFile** result);
};
