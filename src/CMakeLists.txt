add_library(user STATIC user.cpp)
add_library(log STATIC log.cpp)
add_library(engine STATIC engine.cpp)

target_link_libraries(log -lpmem)
target_link_libraries(engine log user)
