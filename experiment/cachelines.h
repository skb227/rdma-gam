#pragma once


#include <remus/remus.h>

using CT = std::shared_ptr<remus::ComputeThread>; 

// enum class to hold the status (can only hold one of the listed values at any given time)
enum class State : uint8_t {
    UNSHARED,       // no other remote caches hold the entry
    SHARED,         // other remote caches have read access
    DIRTY,          // another cache has write access
    INVALID,        // cache line is invalid            (cache line only) 
    U2S,            // in-transition state              (cache line only)
    S2D,            // in-transition state              (cache line only)
    U2D,            // in-transition state              (cache line only)
    D2S,            // in-transition state              (cache line only)
};


// directory entry struct -- lives on the home node of the data, holds metadata
struct DirEntry {
    State flag;                     // unshared / shared / dirty

    // slist (share list)
    uint64_t slist[16];             // at most 16 sharing nodes
    uint64_t slist_cnt;             // count valid sharing nodes, valid only if flag is shared

    // dlist (dirty list) 
    uint64_t dlist[1];              // only one node can have dirty cache line

    // atomic lock to make directory entry accesses atomic (0 = unlocked, 1 = locked)
    remus::Atomic<uint64_t> lock; 


    /// initialize directory entry
    /// @param ct       compute thread context 
    void init (CT &ct) {
        flag = State::UNSHARED;     // set flag initially to UNSHARED (home node only) 
        slist_cnt = 0;              // slist count to 0 
        dlist[0] = -1;              // dlist count to invalid 
        lock.store(0, ct);          // unlock the entry
    }

    /// acquire the lock 
    /// @param ct       compute thread context
    void acquire (CT &ct) {
        while (true) {
            if (lock.compare_exchange_weak(0, 1, ct)) {     // check if value is 0 and set to 1 
                break; 
            }
            while (lock.load(ct) == 1) { /*spin*/ }
        }
    }

    /// release the lock 
    /// @param ct       compute thread context
    void release (CT &ct) {
        lock.store(0, ct); 
    }

    /// call contains on slist 
    /// @param node_id      node id to look for 
    /// @return bool, true if found, false if not 
    bool slist_contains(uint64_t node_id) {
        for (uint64_t i = 0; i < slist_count; i++) {
            if (slist[i] == node_id) return true; 
        }
        return false; 
    }

    /// call add on slist
    /// @param node_id      node id to add to slist
    void slist_add(uint64_t node_id) {
        if (!slist_contains(node_id) && slist_cnt < 16) {
            slist_cnt++; 
            slist[slist_cnt] = node_id; 
        }
    }

    /// call remove on slist
    /// @param node_id      node id to remove from slist
    void slist_remove(uint64_t node_id) {
        for (uint64_t i = 0; i < slist_cnt; i++) {
            if (slist[i] == node_id) {
                slist_cnt--; 
                slist[i] = slist[slist_cnt]; 
            }
        }
    }
};


// cache line -- lives locally on compute node
struct CacheLine {
    uint64_t data;          // locally cached copy of the data
    State flag;             // INVALID / SHARED / DIRTY / in-transition 
};


// keep the directory entry and the data together 
struct DataEntry {
    uint64_t data; 
    DirEntry dir; 

    /// allocate space in RDMA memory, initialize it
    /// @param ct       compute thread context
    /// @param val      value to store
    /// @return rdma_ptr to the memory space
    remus::rdma_ptr<DataEntry> New(uint64_t val, CT &ct) {
        auto mem = ct->New<DataEntry>(); 
        mem->data = val; 
        mem->dir.init(ct);
        return remus::rdma_ptr<DataEntry>((uintptr_t)mem); 
    }
};