#ifndef THINBT_PLATFORM_HPP
#define THINBT_PLATFORM_HPP

#ifdef _WIN32
    #ifndef WIN32_LEAN_AND_MEAN
        #define WIN32_LEAN_AND_MEAN
    #endif
    #include <winsock2.h>
    #include <ws2tcpip.h>
    #include <windows.h>
    #define THINBT_PLATFORM_WINDOWS 1
#else
    #include <unistd.h>
    #include <fcntl.h>
    #include <sys/mman.h>
    #include <sys/sendfile.h>
    #include <arpa/inet.h>
    #define THINBT_PLATFORM_LINUX 1
#endif

#include <cstdint>
#include <string>
#include <vector>

namespace thinbt {

inline uint16_t ntoh16(uint16_t v) { return ntohs(v); }
inline uint32_t ntoh32(uint32_t v) { return ntohl(v); }
inline uint64_t ntoh64(uint64_t v) {
#ifdef _WIN32
    return ((uint64_t)ntohl((uint32_t)v) << 32) | (uint64_t)ntohl((uint32_t)(v >> 32));
#else
    return be64toh(v);
#endif
}
inline uint16_t hton16(uint16_t v) { return htons(v); }
inline uint32_t hton32(uint32_t v) { return htonl(v); }
inline uint64_t hton64(uint64_t v) {
#ifdef _WIN32
    return ((uint64_t)htonl((uint32_t)(v >> 32))) | ((uint64_t)htonl((uint32_t)v) << 32);
#else
    return htobe64(v);
#endif
}

} // namespace thinbt
#endif
