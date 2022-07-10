#include "spdlog/spdlog.h"
#include "log.h"

const char kWALFileName[] = "WAL";

std::string WALFileName(const std::string &dir) {
  std::string filename = dir + "/";
  return filename + kWALFileName;
}

Writer::Writer(PosixWritableFile* dest) : dest_(dest) {}

int Writer::AddRecord(const void* data, int len) {
  // int ret = dest_->Append(data, len);
  // if (ret == 0) {
  //   ret = dest_->Flush();
  // }
  // return ret;
  return dest_->WriteUnbuffered((const char*)data, len);
}

PosixWritableFile* Writer::GetFile() {
  return dest_;
}

Reader::Reader(PosixSequentialFile* file)
    : file_(file), eof_(false), buf_(), backing_store_()
    , buf_start_offset_(0), buf_end_offset_(0) {}

bool Reader::ReadRecord(std::string &record, int len) {
  record.clear();
  // 底层一次read一个kBlockSize的buf
  // 每次先从buf里取记录,取不到则读取下一个kBlockSize的块到buf
  if (!eof_) {
    int cur_record_len = 0;
    if (buf_start_offset_ + RecordSize > buf_end_offset_) {
      // 先读上一个buf尾部不完整的record
      cur_record_len = buf_end_offset_ - buf_start_offset_;
      record.append(buf_.data() + buf_start_offset_, cur_record_len);
      int status = file_->Read(kBlockSize, buf_, backing_store_);
      if (status != 0) {
        return false;
      }
      buf_start_offset_ = 0;
      buf_end_offset_ = buf_.size();
      if (buf_.size() == 0) {
        eof_ = true;
      }
    }
    if (RecordSize - cur_record_len > buf_end_offset_ - buf_start_offset_) {
      if (cur_record_len != 0) {
        spdlog::info("can't read a complete record");
      } // else eof!
      return false;
    }
    record.append(buf_.data() + buf_start_offset_, RecordSize - cur_record_len);
    buf_start_offset_ += RecordSize - cur_record_len;
  } else {
    return false;
  }
  return true;
}
