#include "common/file_util.hpp"
#include <cstring>
#include <stdexcept>

#ifdef THINBT_PLATFORM_LINUX
#include <sys/stat.h>
#include <sys/sendfile.h>
#include <unistd.h>
#include <sys/mman.h>
#endif

namespace thinbt {

MappedFile::~MappedFile() { unmap(); }

bool MappedFile::create_and_map(const std::string& path, uint64_t file_size) {
#ifdef THINBT_PLATFORM_LINUX
    fd_ = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd_ < 0) return false;
    if (fallocate(fd_, 0, 0, file_size) != 0) { close(fd_); fd_ = -1; return false; }
    data_ = static_cast<uint8_t*>(mmap(nullptr, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0));
    size_ = file_size;
    return data_ != MAP_FAILED && data_ != nullptr;
#else
    return false;
#endif
}

bool MappedFile::open_and_map(const std::string& path, bool writable) {
#ifdef THINBT_PLATFORM_LINUX
    int flags = writable ? O_RDWR : O_RDONLY;
    fd_ = open(path.c_str(), flags);
    if (fd_ < 0) return false;
    struct stat st;
    fstat(fd_, &st);
    size_ = st.st_size;
    int prot = writable ? (PROT_READ | PROT_WRITE) : PROT_READ;
    data_ = static_cast<uint8_t*>(mmap(nullptr, size_, prot, MAP_SHARED, fd_, 0));
    return data_ != MAP_FAILED && data_ != nullptr;
#else
    return false;
#endif
}

void MappedFile::unmap() {
#ifdef THINBT_PLATFORM_LINUX
    if (data_ && data_ != MAP_FAILED) { munmap(data_, size_); data_ = nullptr; }
    if (fd_ >= 0) { close(fd_); fd_ = -1; }
#endif
}

bool MappedFile::preallocate(uint64_t size) {
#ifdef THINBT_PLATFORM_LINUX
    return fallocate(fd_, 0, 0, size) == 0;
#else
    (void)size; return false;
#endif
}

bool MappedFile::advise_sequential(uint64_t offset, uint64_t len) {
#ifdef THINBT_PLATFORM_LINUX
    return madvise(static_cast<uint8_t*>(data_) + offset, len, MADV_SEQUENTIAL) == 0;
#else
    (void)offset; (void)len; return false;
#endif
}

bool MappedFile::punch_hole(uint64_t offset, uint64_t len) {
#ifdef THINBT_PLATFORM_LINUX
    return fallocate(fd_, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, len) == 0;
#else
    (void)offset; (void)len; return false;
#endif
}

bool MappedFile::truncate(uint64_t new_size) {
#ifdef THINBT_PLATFORM_LINUX
    return ftruncate(fd_, new_size) == 0;
#else
    (void)new_size; return false;
#endif
}

bool MappedFile::sync() {
#ifdef THINBT_PLATFORM_LINUX
    return msync(data_, size_, MS_SYNC) == 0;
#else
    return false;
#endif
}

bool clone_range(int src_fd, uint64_t src_off, int dst_fd, uint64_t dst_off, uint64_t len) {
#ifdef THINBT_PLATFORM_LINUX
    return copy_file_range(src_fd, reinterpret_cast<off64_t*>(&src_off),
                           dst_fd, reinterpret_cast<off64_t*>(&dst_off),
                           len, 0) == static_cast<ssize_t>(len);
#else
    (void)src_fd; (void)src_off; (void)dst_fd; (void)dst_off; (void)len;
    return false;
#endif
}

ssize_t sendfile_zero_copy(int socket_fd, int file_fd, uint64_t& offset, size_t count) {
#ifdef THINBT_PLATFORM_LINUX
    return sendfile(socket_fd, file_fd, reinterpret_cast<off_t*>(&offset), count);
#else
    (void)socket_fd; (void)file_fd; (void)offset; (void)count;
    return -1;
#endif
}

} // namespace thinbt
