add_library(user STATIC user.cpp)
add_library(plate STATIC plate.cpp)
add_library(engine STATIC engine.cpp)
target_link_libraries(engine plate user)