cmake_minimum_required(VERSION 3.1)
project("interface")
set(CMAKE_CXX_STANDARD 14)

# third_party
add_subdirectory(third_party)
include_directories(third_party/spdlog/include)

# unit test
enable_testing()
add_subdirectory(test)

# generate .so file
include_directories(inc)
add_library(interface
SHARED
        src/interface.cpp
)

# generate test_main
add_executable(test_main src/test_main.cpp)
target_link_libraries(test_main interface)
