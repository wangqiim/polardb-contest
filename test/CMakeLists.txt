add_executable(interface_test interface_test.cpp)
target_link_libraries(interface_test gtest_main interface)

target_include_directories(interface_test PUBLIC
                          "${PROJECT_SOURCE_DIR}/inc"
                          )

add_test(NAME interface_test COMMAND interface_test)

add_executable(plate_test plate_test.cpp)
target_link_libraries(plate_test gtest_main plate)

target_include_directories(plate_test PUBLIC
                          "${PROJECT_SOURCE_DIR}/inc"
                          )

add_test(NAME plate_test COMMAND plate_test)