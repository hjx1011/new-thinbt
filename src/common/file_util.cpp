#include "common/file_util.hpp"
#include <cstring>
#include <stdexcept>

// platform.hpp 头已由 file_util.hpp 引入，提供 Linux/Windows 差异定义
// Windows: thinbt_pread=_pread, thinbt_close=_close, ssize_t=int64_t
// Linux:   thinbt_pread=::pread, thinbt_close=::close

#ifdef _WIN32
#include <windows.h>
#else
#include <sys/stat.h>
#include <sys/sendfile.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>
#endif

namespace thinbt {

MappedFile::~MappedFile() { unmap(); }

MappedFile::MappedFile(MappedFile&& other) noexcept
    : data_(other.data_), size_(other.size_)
{
#ifdef _WIN32
    file_handle_   = other.file_handle_;
    mapping_handle_ = other.mapping_handle_;
    other.file_handle_   = nullptr;
    other.mapping_handle_ = nullptr;
#else
    fd_ = other.fd_;
    other.fd_ = -1;
#endif
    other.data_ = nullptr;
    other.size_ = 0;
}

MappedFile& MappedFile::operator=(MappedFile&& other) noexcept {
    if (this != &other) {
        unmap();
#ifdef _WIN32
        file_handle_   = other.file_handle_;
        mapping_handle_ = other.mapping_handle_;
        other.file_handle_   = nullptr;
        other.mapping_handle_ = nullptr;
#else
        fd_ = other.fd_;
        other.fd_ = -1;
#endif
        data_ = other.data_;
        size_ = other.size_;
        other.data_ = nullptr;
        other.size_ = 0;
    }
    return *this;
}

bool MappedFile::create_and_map(const std::string& path, uint64_t file_size) {
#ifdef _WIN32
    HANDLE h = CreateFileA(path.c_str(), GENERIC_READ | GENERIC_WRITE,
                           0, nullptr, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (h == INVALID_HANDLE_VALUE) return false;

    LARGE_INTEGER li;
    li.QuadPart = static_cast<LONGLONG>(file_size);
    SetFilePointerEx(h, li, nullptr, FILE_BEGIN);
    SetEndOfFile(h);

    HANDLE mapping = CreateFileMappingA(h, nullptr, PAGE_READWRITE,
                                         static_cast<DWORD>(file_size >> 32),
                                         static_cast<DWORD>(file_size & 0xFFFFFFFF), nullptr);
    if (!mapping) {
        CloseHandle(h);
        return false;
    }
    data_ = MapViewOfFile(mapping, FILE_MAP_WRITE, 0, 0, static_cast<size_t>(file_size));
    CloseHandle(mapping);
    if (!data_) {
        CloseHandle(h);
        return false;
    }
    file_handle_ = h;
    mapping_handle_ = nullptr; // 已关闭，保留 file_handle_ 用于 unmap
    size_ = file_size;
    return true;
#else
    fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd_ < 0) return false;
    if (::fallocate(fd_, 0, 0, static_cast<off_t>(file_size)) != 0) {
        ::close(fd_); fd_ = -1; return false;
    }
    data_ = mmap(nullptr, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    size_ = file_size;
    return data_ != MAP_FAILED && data_ != nullptr;
#endif
}

bool MappedFile::open_and_map(const std::string& path, bool writable) {
#ifdef _WIN32
    DWORD access = GENERIC_READ | (writable ? GENERIC_WRITE : 0);
    DWORD share = writable ? 0 : FILE_SHARE_READ;
    HANDLE h = CreateFileA(path.c_str(), access, share, nullptr,
                           OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (h == INVALID_HANDLE_VALUE) return false;

    LARGE_INTEGER li;
    GetFileSizeEx(h, &li);
    uint64_t file_sz = static_cast<uint64_t>(li.QuadPart);

    DWORD fl_protect = writable ? PAGE_READWRITE : PAGE_READONLY;
    DWORD map_access = writable ? FILE_MAP_WRITE : FILE_MAP_READ;
    HANDLE mapping = CreateFileMappingA(h, nullptr, fl_protect,
                                         static_cast<DWORD>(file_sz >> 32),
                                         static_cast<DWORD>(file_sz & 0xFFFFFFFF), nullptr);
    if (!mapping) {
        CloseHandle(h);
        return false;
    }
    data_ = MapViewOfFile(mapping, map_access, 0, 0, static_cast<size_t>(file_sz));
    CloseHandle(mapping);
    if (!data_) {
        CloseHandle(h);
        return false;
    }
    file_handle_ = h;
    size_ = file_sz;
    return true;
#else
    int flags = writable ? O_RDWR : O_RDONLY;
    fd_ = ::open(path.c_str(), flags);
    if (fd_ < 0) return false;
    struct stat st;
    ::fstat(fd_, &st);
    size_ = static_cast<uint64_t>(st.st_size);
    int prot = writable ? (PROT_READ | PROT_WRITE) : PROT_READ;
    data_ = mmap(nullptr, size_, prot, MAP_SHARED, fd_, 0);
    return data_ != MAP_FAILED && data_ != nullptr;
#endif
}

void MappedFile::unmap() {
    if (!data_) return;
#ifdef _WIN32
    UnmapViewOfFile(data_);
    data_ = nullptr;
    if (file_handle_ && file_handle_ != INVALID_HANDLE_VALUE) {
        CloseHandle(file_handle_);
        file_handle_ = nullptr;
    }
    mapping_handle_ = nullptr;
    size_ = 0;
#else
    if (data_ && data_ != MAP_FAILED) { munmap(data_, size_); data_ = nullptr; }
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
#endif
}

bool MappedFile::preallocate(uint64_t size) {
#ifdef _WIN32
    (void)size;
    return false; // Windows 下 create_and_map 已通过 SetEndOfFile 预分配
#else
    return ::fallocate(fd_, 0, 0, static_cast<off_t>(size)) == 0;
#endif
}

bool MappedFile::advise_sequential(uint64_t offset, uint64_t len) {
#ifdef _WIN32
    (void)offset; (void)len;
    return false; // Windows 不支持 madvise，顺序访问由系统自动预读
#else
    return ::madvise(static_cast<uint8_t*>(data_) + offset, len, MADV_SEQUENTIAL) == 0;
#endif
}

bool MappedFile::punch_hole(uint64_t offset, uint64_t len) {
#ifdef _WIN32
    (void)offset; (void)len;
    return false; // Windows 稀疏文件需不同 API，暂不实现
#else
    return ::fallocate(fd_, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                       static_cast<off_t>(offset), static_cast<off_t>(len)) == 0;
#endif
}

bool MappedFile::truncate(uint64_t new_size) {
#ifdef _WIN32
    if (!file_handle_ || file_handle_ == INVALID_HANDLE_VALUE) return false;
    LARGE_INTEGER li;
    li.QuadPart = static_cast<LONGLONG>(new_size);
    if (!SetFilePointerEx(file_handle_, li, nullptr, FILE_BEGIN)) return false;
    if (!SetEndOfFile(file_handle_)) return false;
    size_ = new_size;
    return true;
#else
    if (::ftruncate(fd_, static_cast<off_t>(new_size)) != 0) return false;
    size_ = new_size;
    return true;
#endif
}

bool MappedFile::sync() {
#ifdef _WIN32
    if (!data_) return false;
    return FlushViewOfFile(data_, static_cast<SIZE_T>(size_)) != 0;
#else
    return ::msync(data_, size_, MS_SYNC) == 0;
#endif
}

bool clone_range(int src_fd, uint64_t src_off, int dst_fd, uint64_t dst_off, uint64_t len) {
#ifdef _WIN32
    (void)src_fd; (void)src_off; (void)dst_fd; (void)dst_off; (void)len;
    return false; // Windows 无 copy_file_range，应用层 mmap + memcpy 可替代
#else
    return ::copy_file_range(src_fd, reinterpret_cast<off64_t*>(&src_off),
                             dst_fd, reinterpret_cast<off64_t*>(&dst_off),
                             len, 0) == static_cast<ssize_t>(len);
#endif
}

ssize_t sendfile_zero_copy(int socket_fd, int file_fd, uint64_t& offset, size_t count) {
#ifdef _WIN32
    (void)socket_fd; (void)file_fd; (void)offset; (void)count;
    return -1; // Windows 无 sendfile，需用 TransmitFile 或 mmap + send
#else
    off_t off = static_cast<off_t>(offset);
    ssize_t n = ::sendfile(socket_fd, file_fd, &off, count);
    if (n > 0) offset = static_cast<uint64_t>(off);
    return n;
#endif
}

} // namespace thinbt
