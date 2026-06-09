#ifndef THINBT_SEGMENT_IO_HPP
#define THINBT_SEGMENT_IO_HPP

#include "common/platform.hpp"
#include <string>
#include <vector>
#include <cstdint>

namespace thinbt {

// 最大 chunk 大小（来自 .tseed 格式定义），用于 over-map 的 guard 区域
static constexpr uint64_t MAX_CHUNK_SIZE = 1 * 1024 * 1024;

class SegmentWriter {
public:
    SegmentWriter() = default;
    ~SegmentWriter();

    // 禁止拷贝，允许移动
    SegmentWriter(const SegmentWriter&) = delete;
    SegmentWriter& operator=(const SegmentWriter&) = delete;
    SegmentWriter(SegmentWriter&& other) noexcept;
    SegmentWriter& operator=(SegmentWriter&& other) noexcept;

    // 创建/打开输出文件，预分配磁盘空间，计算 segment 布局
    // file_size 必须 > 0，segment_size 会向上对齐到 MAX_CHUNK_SIZE
    bool open(const std::string& file_path, uint64_t file_size,
              uint64_t segment_size = 64 * 1024 * 1024);

    // 获取 chunk 在 mmap 中的基地址
    // chunk_offset: chunk 在文件中的偏移
    // chunk_size:   chunk 的实际大小（用于边界检查 + over-map 确保覆盖）
    // 返回 nullptr 表示 chunk 无法放入当前映射（参数超出文件范围）
    uint8_t* get_chunk_base(uint64_t chunk_offset, uint32_t chunk_size);

    // 返回文件描述符，供 PeerSession::sendfile 使用
    // fd 在 SegmentWriter 生命周期内保持打开
    int get_file_fd() const { return fd_; }

    // 关闭所有资源：msync + munmap 当前段 + close fd
    void close();

    uint64_t file_size() const     { return file_size_; }
    uint32_t segment_count() const { return segment_count_; }

private:
    struct Segment {
        uint64_t offset;   // 文件内偏移
        uint64_t size;     // 映射大小（含 over-map guard），≤ segment_size_ + MAX_CHUNK_SIZE
    };

    // 切换活跃段：msync 旧段 → munmap 旧段 → mmap 新段
    bool map_segment(uint32_t seg_idx);
    void unmap_current();

    std::string  file_path_;
    uint64_t     file_size_      = 0;
    uint64_t     segment_size_   = 0;   // 对齐后的段大小（MAX_CHUNK_SIZE 的倍数）
    uint32_t     segment_count_  = 0;
    int          fd_             = -1;

    int32_t      active_segment_ = -1;  // 当前 mmap 的段索引
    uint8_t*     active_data_    = nullptr;
    uint64_t     active_size_    = 0;
};

} // namespace thinbt
#endif
