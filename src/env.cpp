#include "env.h"
#include <unistd.h>
#include <fcntl.h>
#include "spdlog/spdlog.h"
#include <algorithm>

int PosixError(const std::string& context, int error_number) {
  if (error_number == ENOENT) {
    spdlog::error("[{}] file not found", context);
    return error_number;
  } else { // IOE
    return 0;
  }
}

//--------------------PosixSequentialFile--------------------------
PosixSequentialFile::PosixSequentialFile(std::string filename, int fd): fd_(fd), filename_(std::move(filename)) {}
PosixSequentialFile::~PosixSequentialFile() { close(fd_); }

int PosixSequentialFile::Read(size_t n, std::string &result, char* scratch) {
  while (true) {
    ::ssize_t read_size = ::read(fd_, scratch, n);
    if (read_size < 0) {  // Read error.
      if (errno == EINTR) {
        continue;  // Retry
      }
      return PosixError(filename_, errno);
    }
    result = std::string(scratch, read_size);
    break;
  }
  return 0;
}

int PosixSequentialFile::Skip(uint64_t n) {
  if (::lseek(fd_, n, SEEK_CUR) == static_cast<off_t>(-1)) {
    return PosixError(filename_, errno);
  }
  return 0;
}

//--------------------PosixWritableFile--------------------------
int PosixWritableFile::SyncFd(int fd, const std::string& fd_path) {
  bool sync_success = ::fsync(fd) == 0;
  if (sync_success) {
    return 0;
  }
  return PosixError(fd_path, errno);
}

PosixWritableFile::PosixWritableFile(std::string filename, int fd)
      : pos_(0), fd_(fd), filename_(std::move(filename)) {}

PosixWritableFile::~PosixWritableFile() {
  if (fd_ >= 0) {
    // Ignoring any potential errors
    Close();
  }
}

int PosixWritableFile::Append(const void* data, const size_t len) {
  size_t write_size = len;
  const char* write_data = (const char*)data;

  // Fit as much as possible into buffer.
  size_t copy_size = std::min(write_size, kWritableFileBufferSize - pos_);
  std::memcpy(buf_ + pos_, write_data, copy_size);
  write_data += copy_size;
  write_size -= copy_size;
  pos_ += copy_size;
  if (write_size == 0) {
    return 0;
  }

  // Can't fit in buffer, so need to do at least one write.
  int status = FlushBuffer();
  if (status != 0) {
    return status;
  }

  // Small writes go to buffer, large writes are written directly.
  if (write_size < kWritableFileBufferSize) {
    std::memcpy(buf_, write_data, write_size);
    pos_ = write_size;
    return 0;
  }
  return WriteUnbuffered(write_data, write_size);
}

int PosixWritableFile::FlushBuffer() {
  int status = WriteUnbuffered(buf_, pos_);
  pos_ = 0;
  return status;
}

int PosixWritableFile::WriteUnbuffered(const char* data, size_t size) {
  while (size > 0) {
    ssize_t write_result = ::write(fd_, data, size);
    if (write_result < 0) {
      if (errno == EINTR) {
        continue;  // Retry
      }
      return PosixError(filename_, errno);
    }
    data += write_result;
    size -= write_result;
  }
  return 0;
}

int PosixWritableFile::Flush() { return FlushBuffer(); }

int PosixWritableFile::Sync() {
  int status = FlushBuffer();
  if (status != 0) {
    return status;
  }
  return SyncFd(fd_, filename_);
}

int PosixWritableFile::Close() {
  int status = FlushBuffer();
  const int close_result = ::close(fd_);
  fd_ = -1;
  if (close_result < 0 && status == 0) {
    status = PosixError(filename_, errno);
  }
  return status;
}
//--------------------PosixEnv--------------------------
int PosixEnv::NewSequentialFile(const std::string& filename, PosixSequentialFile** result) {
  int fd = ::open(filename.c_str(), O_RDONLY);
  if (fd < 0) {
    *result = nullptr;
    return PosixError(filename, errno);
  }

  *result = new PosixSequentialFile(filename, fd);
  return 0;
}

int PosixEnv::NewAppendableFile(const std::string& filename, PosixWritableFile** result) {
  int fd = ::open(filename.c_str(), O_APPEND | O_WRONLY | O_CREAT, 0644);
  if (fd < 0) {
    *result = nullptr;
    return PosixError(filename, errno);
  }

  *result = new PosixWritableFile(filename, fd);
  return 0;
}
