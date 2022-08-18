add_library(user STATIC user.cpp)
add_library(engine STATIC engine.cpp)

target_link_libraries(engine user -lpmem)
