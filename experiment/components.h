#pragma once


#include <remus/remus.h>

using CT = std::shared_ptr<remus::ComputeThread>;

// states of directory and cache line entries
enum State {
    UNSHARED,           // unshared, only home node has access
    SHARED,             // shared, remote nodes have read access
    DIRTY,              // dirty, a remote node has write access
    INVALID,            // cache line invalidated  
    TO_S,                 // cache line in-transition
    TO_D,                 // cache line in-transition
    TO_I,                 // cache line in-transition 
};

// directory entry (exist on home node only) 
struct DirEntry {

    State flag;             // state of the directory entry 

    // slist (share list)  
    uint64_t slist[16];     // the node ids with sharing access (max 16)
    uint64_t slist_cnt;     // number of valid nodes on share list

    // dlist (dirty list)
    uint64_t dlist[1];      // the (singular) node with write access

    // atomic lock to make directory entry accesses atomic (0 = unlocked, 1 = locked)
    remus::Atomic<uint64_t> lock; 

    
    /// initialize directory entry 
    /// @param ct       compute thread context 
    void init(CT &ct) {
        flag = UNSHARED;            // set flag initially to UNSHARED (home node only)
        slist_cnt = 0;              // zero sharing nodes
        dlist[0] = -1;              // no dirty nodes 
        lock.store(0);              // set lock to unlocked (0)  
    }

    /// acquire lock 
    /// @param ct       compute thread context
    void acquire(CT &ct) {
        while(true) {           // keep trying till successful 
            if (lock.compare_exchange_weak(0, 1, ct)) {     // check that value was 0, set to 1
                break; 
            }
            while (lock.load(ct) == 1) { }      // spin till can try again to acquire
        }
    }

    /// release lock 
    /// @param ct       compute thread context
    void release(CT &ct) {
        lock.store(0); 
    }

    /// call contains on slist 
    /// @param nodeID       node id to look for 
    /// @return true if found, false otherwise
    bool slist_contains(uint64_t nodeID) {
        for (int i = 0; i < slist_cnt; i++) {
            if (slist[i] == nodeID) 
                return true; 
        }
        return false; 
    }

    /// call add on slist
    /// @param nodeID       node id to add
    void slist_add(uint64_t nodeID) {
        if(!slist_contains(nodeID) && slist_cnt < 16) {       
                // if that node doesn't already exist on shared list and slist not at max size 
            slist[slist_cnt] = nodeID;
            slist_cnt++; 
        }
    }

    /// call remove on slist 
    /// @param nodeID       node idto remove 
    void slist_remove(uint64_t nodeID) {
        for (uint64_t i = 0; i < slist_cnt; i++) {
            if (slist[i] == nodeID) {
                slist_cnt--; 
                slist[i] = slist[slist_cnt]; 
            }
        }
    }
};

// cache line entry (exist on remote nodes) 
struct CacheLine {

    State flag;             // state of the cache line

    uint64_t home_node;     // home node id

    uint64_t data[64];      // the cached data 
};

// data entry (to couple directory entry with the data itself) 
struct DataEntry {
    uint64_t data[64]; 
    DirEntry dir;           // the metadata for that data line
}