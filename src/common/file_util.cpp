#include "file_util.hpp"

namespace thinbt {

// ---------------------------------------------------------------------------
// MappedFile — stub implementations, full impl in Task 3.6
// ---------------------------------------------------------------------------

MappedFile::~MappedFile() { unmap(); }

MappedFile::MappedFile(MappedFile&&) noexcept = default;
MappedFile& MappedFile::operator=(MappedFile&&) noexcept = default;

bool MappedFile::create_and_map(const std::string&, uint64_t) { return false; }
bool MappedFile::open_and_map(const std::string&, bool)        { return false; }
void MappedFile::unmap() {}

bool MappedFile::preallocate(uint64_t)          { return false; }
bool MappedFile::advise_sequential(uint64_t, uint64_t) { return false; }
bool MappedFile::punch_hole(uint64_t, uint64_t) { return false; }
bool MappedFile::truncate(uint64_t)             { return false; }
bool MappedFile::sync()                         { return false; }

bool clone_range(int, uint64_t, int, uint64_t, uint64_t) { return false; }
uint64_t sendfile_zero_copy(int, int, uint64_t, uint64_t) { return 0; }

} // namespace thinbt
