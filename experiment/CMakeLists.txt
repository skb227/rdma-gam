# example
# build the "lazy list" executable
add_executable(lazylist lazylist.cc)
target_link_libraries(lazylist PRIVATE rdma)