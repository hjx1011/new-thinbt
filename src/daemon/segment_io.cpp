#include "segment_io.hpp"
#include <cstring>
#include <cassert>
#include <iostream>
#include <algorithm>

#ifdef _WIN32
#include <io.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#endif

namespace thinbt {

SegmentWriter::~SegmentWriter() {
    close();
}

SegmentWriter::SegmentWriter(SegmentWriter&& other) noexcept
    : file_path_(std::move(other.file_path_))
    , file_size_(other.file_size_)
    , segment_size_(other.segment_size_)
    , segment_count_(other.segment_count_)
    , fd_(other.fd_)
    , mapped_segments_(std::move(other.mapped_segments_))
    , cached_seg_idx_(other.cached_seg_idx_)
    , cached_seg_(other.cached_seg_)
{
    other.fd_             = -1;
    other.cached_seg_idx_ = UINT32_MAX;
    other.cached_seg_     = nullptr;
}

SegmentWriter& SegmentWriter::operator=(SegmentWriter&& other) noexcept {
    if (this != &other) {
        close();
        file_path_       = std::move(other.file_path_);
        file_size_       = other.file_size_;
        segment_size_    = other.segment_size_;
        segment_count_   = other.segment_count_;
        fd_              = other.fd_;
        mapped_segments_ = std::move(other.mapped_segments_);
        cached_seg_idx_  = other.cached_seg_idx_;
        cached_seg_      = other.cached_seg_;
        other.fd_             = -1;
        other.cached_seg_idx_ = UINT32_MAX;
        other.cached_seg_     = nullptr;
    }
    return *this;
}

bool SegmentWriter::open(const std::string& file_path, uint64_t file_size,
                         uint64_t segment_size) {
    if (file_size == 0) {
        std::cerr << "[SegmentWriter] file_size must be > 0" << std::endl;
        return false;
    }

    // 段大小至少为 MAX_CHUNK_SIZE，保证 over-map guard 不会过度重叠
    // 不对齐到 MAX_CHUNK_SIZE 倍数——over-map 策略本身处理跨段边界问题
    if (segment_size < MAX_CHUNK_SIZE) segment_size = MAX_CHUNK_SIZE;

    file_path_     = file_path;
    file_size_     = file_size;
    segment_size_  = segment_size;
    segment_count_ = static_cast<uint32_t>((file_size + segment_size - 1) / segment_size);

#ifdef _WIN32
    // Windows: 检查已有文件大小，决定是否需要重建
    HANDLE h = CreateFileA(file_path.c_str(), GENERIC_READ | GENERIC_WRITE,
                           0, nullptr, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (h == INVALID_HANDLE_VALUE) {
        std::cerr << "[SegmentWriter] CreateFile failed: " << file_path << std::endl;
        return false;
    }
    LARGE_INTEGER existing_size;
    GetFileSizeEx(h, &existing_size);
    if (existing_size.QuadPart != static_cast<LONGLONG>(file_size)) {
        // 文件不存在或大小不匹配，重新预分配
        LARGE_INTEGER li;
        li.QuadPart = static_cast<LONGLONG>(file_size);
        SetFilePointerEx(h, li, nullptr, FILE_BEGIN);
        SetEndOfFile(h);
        SetFileValidData(h, li.QuadPart);
    }
    CloseHandle(h);
    fd_ = _open(file_path.c_str(), _O_RDWR | _O_BINARY);
#else
    // Linux: 检查已有文件大小，存在且匹配则保留数据，否则重建
    struct stat st;
    bool file_exists = (::stat(file_path.c_str(), &st) == 0);
    bool size_matches = file_exists && (static_cast<uint64_t>(st.st_size) == file_size);

    int flags = O_RDWR | O_CREAT;
    if (!size_matches) flags |= O_TRUNC;  // 大小不匹配才截断

    fd_ = ::open(file_path.c_str(), flags, 0644);
    if (fd_ < 0) {
        std::cerr << "[SegmentWriter] open failed: " << file_path << std::endl;
        return false;
    }

    if (!size_matches) {
        // 新文件或大小不匹配：预分配磁盘空间
        if (::fallocate(fd_, 0, 0, static_cast<off_t>(file_size)) != 0) {
            std::cerr << "[SegmentWriter] fallocate failed, falling back to ftruncate" << std::endl;
            if (::ftruncate(fd_, static_cast<off_t>(file_size)) != 0) {
                std::cerr << "[SegmentWriter] ftruncate failed" << std::endl;
                ::close(fd_);
                fd_ = -1;
                return false;
            }
        }
    }
#endif

    return true;
}

uint8_t* SegmentWriter::get_chunk_base(uint64_t chunk_offset, uint32_t chunk_size) {
    if (fd_ < 0) return nullptr;
    if (chunk_offset + chunk_size > file_size_) return nullptr;
    if (chunk_size > MAX_CHUNK_SIZE) {
        std::cerr << "[SegmentWriter] chunk_size " << chunk_size
                  << " exceeds MAX_CHUNK_SIZE " << MAX_CHUNK_SIZE << std::endl;
        return nullptr;
    }

    uint32_t seg_idx = static_cast<uint32_t>(chunk_offset / segment_size_);
    if (seg_idx >= segment_count_) return nullptr;

    // 快速路径：连续访问同一段，无需查 map
    MappedSeg* seg = nullptr;
    if (seg_idx == cached_seg_idx_) {
        seg = cached_seg_;
    } else {
        auto it = mapped_segments_.find(seg_idx);
        if (it == mapped_segments_.end()) {
            // 段尚未映射 → mmap 并加入缓存，不 unmap 任何已有段
            if (!map_segment(seg_idx)) return nullptr;
            it = mapped_segments_.find(seg_idx);
        }
        seg = &it->second;
        cached_seg_idx_ = seg_idx;
        cached_seg_     = seg;
    }

    uint64_t offset_in_seg = chunk_offset - static_cast<uint64_t>(seg_idx) * segment_size_;
    if (offset_in_seg + chunk_size > seg->size) {
        std::cerr << "[SegmentWriter] chunk crosses guard boundary" << std::endl;
        return nullptr;
    }

    return seg->data + offset_in_seg;
}

void SegmentWriter::close() {
    unmap_all();

    if (fd_ >= 0) {
#ifdef _WIN32
        ::_close(fd_);
#else
        ::close(fd_);
#endif
        fd_ = -1;
    }

    file_size_     = 0;
    segment_size_  = 0;
    segment_count_ = 0;
}

// ── private ──

bool SegmentWriter::map_segment(uint32_t seg_idx) {
    // 不 unmap 任何已有段 — 多个段同时映射，避免 ChunkAssembler 悬空指针
    uint64_t seg_start = static_cast<uint64_t>(seg_idx) * segment_size_;
    uint64_t map_size = std::min(segment_size_ + MAX_CHUNK_SIZE, file_size_ - seg_start);

    MappedSeg seg;
    seg.size = map_size;

#ifdef _WIN32
    HANDLE h = (HANDLE)_get_osfhandle(fd_);
    HANDLE mapping = CreateFileMappingA(h, nullptr, PAGE_READWRITE,
                                         static_cast<DWORD>(map_size >> 32),
                                         static_cast<DWORD>(map_size & 0xFFFFFFFF), nullptr);
    if (!mapping) {
        std::cerr << "[SegmentWriter] CreateFileMapping failed" << std::endl;
        return false;
    }
    seg.data = static_cast<uint8_t*>(MapViewOfFile(mapping, FILE_MAP_WRITE,
                                       0, 0, static_cast<size_t>(map_size)));
    CloseHandle(mapping);
    if (!seg.data) {
        std::cerr << "[SegmentWriter] MapViewOfFile failed" << std::endl;
        return false;
    }
#else
    seg.data = static_cast<uint8_t*>(::mmap(
        nullptr, static_cast<size_t>(map_size),
        PROT_READ | PROT_WRITE, MAP_SHARED,
        fd_, static_cast<off_t>(seg_start)));

    if (seg.data == MAP_FAILED) {
        std::cerr << "[SegmentWriter] mmap failed for segment " << seg_idx
                  << " offset=" << seg_start << " size=" << map_size << std::endl;
        return false;
    }

    ::madvise(seg.data, static_cast<size_t>(map_size), MADV_WILLNEED);
#endif

    mapped_segments_[seg_idx] = seg;
    return true;
}

void SegmentWriter::unmap_all() {
    cached_seg_     = nullptr;
    cached_seg_idx_ = UINT32_MAX;

    for (auto& [idx, seg] : mapped_segments_) {
        if (!seg.data) continue;
#ifdef _WIN32
        FlushViewOfFile(seg.data, static_cast<SIZE_T>(seg.size));
        UnmapViewOfFile(seg.data);
#else
        ::msync(seg.data, static_cast<size_t>(seg.size), MS_SYNC);
        ::munmap(seg.data, static_cast<size_t>(seg.size));
#endif
    }
    mapped_segments_.clear();
}

} // namespace thinbt
