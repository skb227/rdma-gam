#pragma once

#include <unordered_map>
#include <mutex>
#include <remus/remus.h>
#include "cache.h"

using CT = std::shared_ptr<std::ComputeThread>; 

class GAMcache {

    std::unordered_map<uint64_t, CacheLine> cache_map; 
    std::mutex mtx_lock;            // for accessing cache_map
    uint64_t thisID;                // this node's id

public: 
    
    GAMcache(uint64_t node_id) : node_id(node_id) {}

    /// check if the rdma_ptr points to this node id
    /// @param ptr      the rdma ptr to a DataEntry 
    bool isNh(remus::rdma_ptr<DataEntry> ptr) {
        return ptr.id() == node_id;         // check that rdma ptr node is the same as this node
    }

    /// checks if the addr is cached locally
    /// @param addr     addr to look for
    /// @param out      where to store the location
    /// @return true if addr is cached locally, false otherwise
    bool cache_lookup(uint64_t addr, CacheLine &out) {
        std::lock_guard<std::mutex> guard(mtx_lock); 
        auto itr = cache_map.find(addr); 
        if (itr != cache_map.end()) {
            out = itr->second; 
            return true; 
        }
        return false; 
    }

    /// insert cache line into cache map 
    /// @param addr     the addr to add to
    /// @param line     the cache line to add
    void cache_insert(uint64_t addr, CacheLine &line) {
        std::lock_guard<std::mutex> guard(mtx_lock); 
        cache_map[addr] = line; 
    }

    /// sets the state of the cache line to invalid 
    /// @param addr     the addr of the cache line to invalidate 
    void cache_invalidate(uint64_t addr) {
        std::lock_guard<std::mutex> guard(mtx_lock); 
        auto itr = cache_map.find(addr); 
        if (itr != cache_map.end())
            itr->second.flag = INVALID; 
    }

    /// read the data at the addr
    /// @param ptr      rdma_ptr to the data entry to read from 
    /// @param ct       compute thread context
    /// @return the data read from the addr 
    uint64_t read(remus::rdma_ptr<DataEntry> ptr, CT &ct);

    /// write data to the addr 
    /// @param ptr      rdma_ptr to write the data entry to
    /// @param val      data to write 
    /// @param ct       compute thread context
    void write(remus::rdma_ptr<DataEntry> ptr, uint64_t val, CT &ct); 
}