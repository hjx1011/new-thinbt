#ifndef THINBT_FILE_UTIL_HPP
#define THINBT_FILE_UTIL_HPP

#include "platform.hpp"

#include <cstddef>
#include <cstdint>
#include <string>

namespace thinbt {

class MappedFile {
public:
    MappedFile() = default;
    ~MappedFile();

    MappedFile(const MappedFile&) = delete;
    MappedFile& operator=(const MappedFile&) = delete;
    MappedFile(MappedFile&&) noexcept;
    MappedFile& operator=(MappedFile&&) noexcept;

    bool create_and_map(const std::string& path, uint64_t file_size);
    bool open_and_map(const std::string& path, bool writable = false);
    void unmap();

    void*       data()       { return data_; }
    const void* data() const { return data_; }
    uint64_t    size() const { return size_; }
#ifdef _WIN32
    int         fd()   const { return -1; }
#else
    int         fd()   const { return fd_; }
#endif

    bool preallocate(uint64_t size);
    bool advise_sequential(uint64_t offset, uint64_t len);
    bool punch_hole(uint64_t offset, uint64_t len);
    bool truncate(uint64_t new_size);
    bool sync();

private:
#ifdef _WIN32
    void*   file_handle_   = nullptr;
    void*   mapping_handle_ = nullptr;
#else
    int     fd_   = -1;
#endif
    void*   data_ = nullptr;
    uint64_t size_ = 0;
};

bool clone_range(int src_fd, uint64_t src_off,
                 int dst_fd, uint64_t dst_off, uint64_t len);

ssize_t sendfile_zero_copy(socket_handle_t socket_fd, int file_fd,
                            uint64_t& offset, size_t count);

} // namespace thinbt

#endif
