add_library(yyjson STATIC yyjson/yyjson.c)
target_include_directories(yyjson PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/yyjson)

add_library(moodycamel INTERFACE)
target_include_directories(moodycamel INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/moodycamel)
