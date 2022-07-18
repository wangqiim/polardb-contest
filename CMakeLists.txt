cmake_minimum_required(VERSION 3.1)
project("interface")
set(CMAKE_CXX_STANDARD 14)
add_definitions(-D_FILE_OFFSET_BITS=64)
add_compile_options(-fPIC)

set(CMAKE_CXX_FLAGS "-Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-Ofast")

# Set a default build type if none was specified
set(default_build_type "Release")

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to '${default_build_type}' as none was specified.")
  set(CMAKE_BUILD_TYPE "${default_build_type}" CACHE
      STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
    "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()

include_directories(src/include)

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
        src/plate_interface.cpp
)
target_link_libraries(interface engine)

# src
add_subdirectory(src)

# generate test_main
add_executable(test_main src/test_main.cpp)
target_link_libraries(test_main interface)
