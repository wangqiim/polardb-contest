add_executable(interface_test interface_test.cpp)
target_link_libraries(interface_test gtest_main interface)

target_include_directories(interface_test PUBLIC
                          "${PROJECT_SOURCE_DIR}/inc"
                          )

add_test(NAME interface_test COMMAND interface_test)

add_executable(leveldb_test leveldb_test.cpp)
target_link_libraries(leveldb_test gtest_main leveldb)
target_include_directories(leveldb_test PUBLIC
                          "${PROJECT_SOURCE_DIR}/inc"
                          )
add_test(NAME leveldb_test COMMAND leveldb_test)
                          
