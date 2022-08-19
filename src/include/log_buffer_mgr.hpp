#include <queue>
#include <mutex>
#include <unistd.h>
#include <cstdint>
#include <thread>
#include <condition_variable>
#include <string.h>
#include <xmmintrin.h>
#include <sys/mman.h>
#include <libpmem.h>
#include <chrono>

#include "spdlog/spdlog.h"

#include "def.h"
#include "util.h"

// 调参: 本地运行需要调小
const uint64_t PmemSize = 16 * (1024ULL * 1024ULL * 1024ULL); // 16G 搓搓有余
// ------ log_buffer_mgr.hpp -------
const uint64_t LogBufferMgrSize = 6 * (1024ULL * 1024ULL * 1024ULL); // pagecache: 6.4G?? (32 * 0.2)
const uint64_t LogBufferMgrHeaderSize = 4ULL + 4ULL; // LogbufferMgrHeader记录目前已经落盘(pmem)了多少条记录，以及正在落盘哪个bufferlog
const uint64_t LogBufferMgrBody = LogBufferMgrSize - LogBufferMgrHeaderSize;

const uint64_t LogBufferHeaderSize = 8ULL; // 当前buffer log已经commit了多少条记录
const uint64_t LogBufferBodySize = 272ULL * 256ULL * 100ULL; // (272 * 256) LogBufferSize = LCM(272, 256), 必须是倍数(log buffer需要判断是否满)
const uint64_t LogBufferSize = LogBufferHeaderSize + LogBufferBodySize;

const uint64_t RocordNumPerBuffer = LogBufferBodySize / RecordSize;
const uint64_t LogBufferFreeNumPerBufferMgr = LogBufferMgrBody / LogBufferSize; 

// unused
const uint64_t LogBufferMgrRemainSize = LogBufferMgrBody % LogBufferSize; // todo(wq): 尽量不要让mmap(LogBufferMgrSize)有剩余

const uint32_t FlushingNone = 0x00000000UL;

class MmapWrapper {
public:
    // 构造时保证创建文件并且alloc大小, 如果是第一次创建memset(0)，并且warmup整块mmap内存
  MmapWrapper(const std::string &filename, uint64_t mmap_size)
							: filename_(filename), mmap_size_(mmap_size), fd_(-1)
							, start_(nullptr) {
		Util::CreateIfNotExists(filename_);
		// 1. open fd; (must have been create)
		fd_ = open(filename_.c_str(), O_RDWR, 0644);
		if (fd_ < 0) {
				spdlog::error("[MmapWrapper] can't open file {}", filename_);
				exit(1);
		}
		uint64_t off = (uint64_t)lseek(fd_, 0, SEEK_END);
		if (off < 0) {
				spdlog::error("[MmapWrapper] lseek end failed");
				exit(1);
		}
		if (off == 0) {
				if (posix_fallocate(fd_, 0, mmap_size_) != 0) {
				spdlog::error("[MmapWrapper] posix_fallocate failed");
				exit(1);
				}
		}
		// 2. mmap
		void* ptr = mmap(NULL, mmap_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
		if (ptr == MAP_FAILED) {
				spdlog::error("[MmapWrapper] mmap failed, errno is {}", strerror(errno));
				exit(1);
		}
		if (off == 0) {
				memset(ptr, 0, mmap_size_);
		}
		start_ = reinterpret_cast<char *>(ptr);
		warmUpAll();
	}

  ~MmapWrapper() {
    munmap(start_, mmap_size_);
    close(fd_);
  }
  
char *GetOffset(uint64_t offset) { return start_ + offset; }

private:
  void warmUpAll() { 
    for (int offset = 0; offset < mmap_size_; offset += 64) {
        _mm_prefetch((const void *)(start_ + offset), _MM_HINT_T0);
    }
  }

private:
  const std::string filename_;
  uint64_t mmap_size_;
  int fd_;
  char *start_;
};

class PmemWrapper {
 public:
  PmemWrapper(const std::string &filename, uint64_t pool_size)
				: filename_(filename), pool_size_(pool_size), fd_(-1)
				, start_(nullptr) {
		// 1. create file if not exist
		bool create_file = !Util::FileExists(filename_);
		// 2. pmap
		void* pmemaddr = NULL;
		size_t mapped_len;
		int is_pmem;
		if ((pmemaddr = pmem_map_file(filename_.c_str(), pool_size_,
					PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem)) == NULL) {
			perror("pmem_map_file");
			exit(1);
		}
		if (mapped_len != pool_size_ || is_pmem == 0) {
			spdlog::warn("[PmemWrapper] unexpected error happen when pmem_map_file, mapped_len: {}, is_pmem: {}", mapped_len, is_pmem);
		}
		start_ = reinterpret_cast<char *>(pmemaddr);
		if (create_file) {
			pmem_memset_nodrain(start_, 0, pool_size_);
			pmem_drain();
		}
	}

  ~PmemWrapper() {
		pmem_drain();
	  pmem_unmap(start_, pool_size_);
	}

	// 每次flush一大块到pmem中去
	void Copy(char *dst, const char *src, uint64_t len = LogBufferBodySize) {
  		pmem_memcpy_nodrain(dst, src, len);
	}

	char *GetOffset(uint64_t offset) { return start_ + offset; }

 private:
  const std::string filename_;
  uint64_t pool_size_;
  int fd_;
  char *start_;
};

// 为了恢复方便，mgr为每个buffer的heder不是置为0, 而是将8字节拆成2个4字节
// [uint_32, uint_32]=[该buffer的第一次写将对应总日志的第几条, 该buffer已经被写了几条记录]
class LogBuffer {
public:
    LogBuffer(char *buffer_start, uint64_t size, uint32_t no) {
        commit_cnt_ = reinterpret_cast<uint64_t *>(buffer_start);
        start_ = buffer_start + LogBufferHeaderSize;
        curr_ = start_ + (*commit_cnt_) * RecordSize;
        data_size_ = size - LogBufferHeaderSize;
		no_ = no;
    }

    void Reset() {
		*commit_cnt_ = 0;
		curr_ = start_;
	};

    bool IsFull() { return uint64_t(curr_ - start_) == data_size_; };

    uint64_t CommitCnt() { return *commit_cnt_; };
    void SetFull() {  
		*commit_cnt_ = RocordNumPerBuffer; 
		curr_ = start_ + data_size_;
	};

	char *DataStartPtr() { return start_; }

	uint32_t No() { return no_; }
    // 每次默认append长度为RecordSize(172)
    // data: append的数据
    // 返回值：0: 成功
    int Append(const void *data) {
        if (IsFull()) {
            return -1;
        }
        memcpy(curr_, data, RecordSize);
        curr_ += RecordSize;
        *commit_cnt_ += 1;
		return 0;
    }

private:
	uint64_t *commit_cnt_; // 该页提交了多少条记录
	char     *start_;
	char     *curr_;
	uint64_t data_size_;
	uint32_t no_;
};

class LogBufferMgrHeader {
public:
	LogBufferMgrHeader(uint32_t flushed_cnt, uint32_t flushing_no)
		: flushed_cnt_(flushed_cnt), flushing_no_(flushing_no) { }
	void AtomicSet(uint32_t flushed_cnt, uint32_t flushing_no) {
		uint64_t tmp;
		*(uint32_t *)(&tmp) = flushed_cnt;
		*((uint32_t *)(&tmp) + 1) = flushing_no;
		*(uint64_t *)this = tmp; // 原子写入，防止崩溃
	}
	uint32_t flushed_cnt_; // 记录目前已经确定的flushing了多少条记录到pmem中
	uint32_t flushing_no_; // 用来崩溃恢复（因为可能在flush某个log buffer时崩溃)
};
// LogBufferMgr中mmap的存储结构
// ______________________________________________________________________________________________________________________________________________
// | flushed_cnt(4bytes), flushing_no_(4bytes) | commit_cnt_(8bytes), char[log_buffer_body] | commit_cnt_(8bytes), char[log_buffer_body] | ......
//
// LogBufferMgr中pmem的存储结构  __________________________________________________________________
// 全是record，无其他字段,       | record(172bytes) | record(172bytes) | record(172bytes) | ......
class LogBufferMgr {
public:
	LogBufferMgr(const char *mmap_filename, const char *pmem_filename,
							uint64_t log_buffer_size = LogBufferMgrSize, uint64_t pmem_size = PmemSize) {
		mmap_wrapper_ = new MmapWrapper(mmap_filename, log_buffer_size);
		pmem_wrapper_ = new PmemWrapper(pmem_filename, pmem_size);

		flush_thread_ = nullptr;
		is_stop_ = false;

		header_ = reinterpret_cast<LogBufferMgrHeader *>(mmap_wrapper_->GetOffset(0));
		char *start = mmap_wrapper_->GetOffset(LogBufferMgrHeaderSize);
		char *end = mmap_wrapper_->GetOffset(log_buffer_size);
		uint32_t no = 1;
		for (char *curr = start; curr + LogBufferSize <= end; curr += LogBufferSize, no += 1) {
			// 重启时全部置为可分配，被拿走消费的时候发现写满了再归还
			LogBuffer *log_buffer = new LogBuffer(curr, LogBufferSize, no);
			own_log_buffers_.push_back(log_buffer);
			free_log_buffer_.push(log_buffer);
		}
	}

	~LogBufferMgr() {
		is_stop_ = true;
		flush_thread_->join();
		delete(flush_thread_);

		for (size_t i = 0; i < own_log_buffers_.size(); i++) {
			delete own_log_buffers_[i];
		}
		delete mmap_wrapper_;
		delete pmem_wrapper_;
	}

	// 阻塞调用： 获取一个空闲页用作buffer
	LogBuffer* GetFreeLogBuffer() {
			std::unique_lock<std::mutex> guard(free_log_buffer_mtx_);
			while (free_log_buffer_.empty()) {
					free_log_buffer_cv_.wait(guard);
			}
			LogBuffer* log_buffer = free_log_buffer_.front();
			free_log_buffer_.pop();
			return log_buffer;
	}

	// 阻塞调用, 归还一个脏页
	void GiveBackDirtyBuffer(LogBuffer* log_buffer) {
			std::unique_lock<std::mutex> guard(dirty_log_buffer_mtx_);
			dirty_log_buffer_.push(log_buffer);
			dirty_log_buffer_cv_.notify_one(); // todo(wq): notify_one is ok?
	}

    // todo(wq): 设计成无锁
    // 阻塞调用, 归还一个自由页，被background线程调用
    void GiveBackFreeBuffer(LogBuffer* log_buffer) {
        std::unique_lock<std::mutex> guard(free_log_buffer_mtx_);
        free_log_buffer_.push(log_buffer);
        free_log_buffer_cv_.notify_one(); // todo(wq): notify_one is ok?
    }

	void StartFlushRun() {
        // todo(wq): mgr析构时需要join这个线程吗???
        flush_thread_ = new std::thread(&LogBufferMgr::backGroudFlush, this);
    }

	char* GetPmemStart() { return pmem_wrapper_->GetOffset(0); }

	LogBufferMgrHeader* Header() { return header_; }

	LogBuffer* GetLogBufferByNo(uint32_t no) {
		return own_log_buffers_[no];
	}
private:
    // todo(wq): 无锁设计
    // 该方法不能永久阻塞，以为后台刷线程需要定期醒来判断是否被停止
    LogBuffer* getDirtyLogBuffer() {
        std::unique_lock<std::mutex> guard(dirty_log_buffer_mtx_);
        if (dirty_log_buffer_.empty()) {
			using namespace std::chrono_literals;
            dirty_log_buffer_cv_.wait_for(guard, 1000ms);
        }
		if (dirty_log_buffer_.empty()) { // 超时醒来的
			return nullptr;
		}
        LogBuffer* log_buffer = dirty_log_buffer_.front();
        dirty_log_buffer_.pop();
        return log_buffer;
    }


    // 开启后台线程刷dirty_buffer
    void backGroudFlush() {
        for (;;) {
            // 1. 阻塞调用，获取一个DirtyLogBuffer
            LogBuffer *dirty_log_buffer = nullptr;
			while ((dirty_log_buffer = getDirtyLogBuffer()) == nullptr) {
				if (is_stop_) {
					return;
				}
			}
            // 2. 刷盘
			header_->AtomicSet(header_->flushed_cnt_, dirty_log_buffer->No());
            flush(dirty_log_buffer);
			// 3. 先改logbuffer header再改logbufferMgr，这样可以保证: 如果在改完log buffer header之后崩溃，
			// 恢复的时候，则重新刷一次该log buffer也无妨
            dirty_log_buffer->Reset();
			header_->AtomicSet(header_->flushed_cnt_ + RocordNumPerBuffer, FlushingNone);
			// 4. 将该buffer归还到free队列里
			GiveBackFreeBuffer(dirty_log_buffer);
        }
    }

	// 根据mmap的起始偏移地址，推算出是第几个
	uint32_t offset2No(LogBuffer* log_buffer) {
		return (((char *)log_buffer - mmap_wrapper_->GetOffset(LogBufferMgrHeaderSize)) / LogBufferSize) + 1;
	}

    void flush(LogBuffer *dirty_log_buffer) {
		uint64_t dst_offset = header_->flushed_cnt_ * RecordSize;
		char *dst = pmem_wrapper_->GetOffset(dst_offset);
		char *data_src = dirty_log_buffer->DataStartPtr();
		pmem_wrapper_->Copy(dst, data_src, LogBufferBodySize);
    }

private:
	MmapWrapper *mmap_wrapper_;
	PmemWrapper *pmem_wrapper_;

	std::thread *flush_thread_;
	bool is_stop_;

	std::vector<LogBuffer *> own_log_buffers_;

	LogBufferMgrHeader *header_; // 记录目前已经被刷了多少条记录了

	// todo(wq): 优化锁的粒度，比如无锁队列 + 自旋
	std::mutex free_log_buffer_mtx_;
	std::condition_variable free_log_buffer_cv_;
	std::queue<LogBuffer *> free_log_buffer_;

	std::mutex dirty_log_buffer_mtx_;
	std::condition_variable dirty_log_buffer_cv_;
	std::queue<LogBuffer *> dirty_log_buffer_; // 已经写满了，需要刷盘
};

class LogWriter {
public:
    LogWriter(LogBufferMgr *log_buffer_mgr)
        : log_buffer_mgr_(log_buffer_mgr)
        , log_buffer_(log_buffer_mgr_->GetFreeLogBuffer()) {
    }

	~LogWriter() {
		log_buffer_mgr_->GiveBackFreeBuffer(log_buffer_);
	}
    
    int Append(const void *data) {
		// 这里循环的原因: 可能不巧mgr连续分配的log_buffer都是写满的，比如所有log_buffer都被写满，然后被kill
        while (log_buffer_->Append(data) != 0) {
			log_buffer_mgr_->GiveBackDirtyBuffer(log_buffer_);
			log_buffer_ = log_buffer_mgr_->GetFreeLogBuffer();
        }
		return 0;
    }
private:
    LogBufferMgr *log_buffer_mgr_;
    LogBuffer    *log_buffer_;
};

class LogReader {
public:
    LogReader(LogBufferMgr *log_buffer_mgr)
        : log_buffer_mgr_(log_buffer_mgr)
		, next_no_(0)
		, log_buffer_read_cnt_(0)
        , curr_log_buffer_(log_buffer_mgr_->GetLogBufferByNo(next_no_++))
		, pmem_start_(log_buffer_mgr_->GetPmemStart())
		, header_(log_buffer_mgr_->Header())
		, totol_read_cnt_(0) {
		// 如果flushing_no对应的log page正在刷，则将其commit_cnt置满，重复刷一次也无妨，反正都在同一个位置
		if (header_->flushing_no_ != FlushingNone) {
			LogBuffer* flushing_log_buffer = log_buffer_mgr_->GetLogBufferByNo(header_->flushing_no_ - 1);
			flushing_log_buffer->SetFull(); // 置满
		}
    }
    
    int ReadRecord(char *&record, int len = RecordSize) {
		// 1. 先读pmem
		if (totol_read_cnt_ < header_->flushed_cnt_) {
			record = pmem_start_;
			pmem_start_ += len;
			totol_read_cnt_++;
			return 0;
		}
		// 2. 再读所有log_buffer
		if (log_buffer_read_cnt_ < curr_log_buffer_->CommitCnt()) {
			record = curr_log_buffer_->DataStartPtr() + log_buffer_read_cnt_ * len;
			log_buffer_read_cnt_++;
			totol_read_cnt_++;
			return 0;
		}
		while (log_buffer_read_cnt_ == curr_log_buffer_->CommitCnt()) {
			if (next_no_ < LogBufferFreeNumPerBufferMgr) {
				curr_log_buffer_ = log_buffer_mgr_->GetLogBufferByNo(next_no_++);
				log_buffer_read_cnt_ = 0;
			} else { // 所有的logbuffer都尝试过了，buffer里的数据被读完了
				return -1;
			}
		}
		record = curr_log_buffer_->DataStartPtr() + log_buffer_read_cnt_ * len;
		log_buffer_read_cnt_++;
		totol_read_cnt_++;
		return 0;
    }
private:
    LogBufferMgr *log_buffer_mgr_;
	
	uint32_t  next_no_;
	uint32_t  log_buffer_read_cnt_;
    LogBuffer *curr_log_buffer_;

	char *pmem_start_;
	LogBufferMgrHeader *header_;
	uint32_t totol_read_cnt_;
};
