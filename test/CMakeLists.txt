add_executable(interface_test interface_test.cpp)
target_link_libraries(interface_test gtest_main interface)

target_include_directories(interface_test PUBLIC
                          "${PROJECT_SOURCE_DIR}/inc"
                          )

add_test(NAME interface_test COMMAND interface_test)

add_executable(interface_concurrent_test interface_concurrent_test.cpp)
target_link_libraries(interface_concurrent_test gtest_main interface)

target_include_directories(interface_concurrent_test PUBLIC
                          "${PROJECT_SOURCE_DIR}/inc"
                          )

add_test(NAME interface_concurrent_test COMMAND interface_concurrent_test)


add_executable(log_test log_test.cpp)
target_link_libraries(log_test gtest_main log)

target_include_directories(log_test PUBLIC
                          "${PROJECT_SOURCE_DIR}/inc"
                          )

add_test(NAME log_test COMMAND log_test)