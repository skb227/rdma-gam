#pragma once

#include <unordered_map>
#include <mutex> 
#include <remus/remus.h>

class GAMcache {

    // cache map -- kv pairs, unique keys 
    std::unordered_map<uint64_t, CacheLine> cache_map; 

    // muetx lock 
    std::mutex mtx_lock; 

    // this node id (each node has one gam cache instance)
    uint64_t thisID; 

public: 

    // 'This' to access consistent RDMA memory 
    GAMcache *This; 

    /// initialize instance (one per node) -- construct a GAMcache
    ///             setting its 'This' pointer to a remote memory location
    /// @param This
    /// @param nodeid       this node's ID 
    GAMcache(const remus::rdma_ptr<GAMcache> &This, uint64_t nodeid) 
        : This((GAMcache *)((uintptr_t)This)), thisID(nodeid) {}

    

    /// READING DATA

}