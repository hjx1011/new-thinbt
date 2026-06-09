add_library(yyjson INTERFACE)
target_include_directories(yyjson INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/yyjson)

add_library(moodycamel INTERFACE)
target_include_directories(moodycamel INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/moodycamel)
