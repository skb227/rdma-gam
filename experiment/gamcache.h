#pragma once

#include <unordered_map>
#include <mutex>
#include <remus/remus.h>
#include "cache.h"

using CT = std::shared_ptr<remus::ComputeThread>; 

class GAMcache {

    std::unordered_map<uint64_t, CacheLine> cache_map; 
    std::mutex mtx_lock;            // for accessing cache_map
    uint64_t thisID;                // this node's id

    // for mailbox and requests
    remus::rdma_ptr<Mailbox> mailboxes; 
    std::atomic<uint64_t> nextReqID{0}; 

    // for incoming responses 
    std::unordered_map<uint64_t, Message> resp_map; 


public: 
    
    GAMcache(uint64_t node_id, remus::rdma_ptr<Mailbox> mbox) 
        : thisID(node_id), mailboxes(mbox) {}
    
    
    ///     working with the mailbox 
    uint64_t getReqID() { return nextReqID.fetch_add(1); }

    /// send a message request 
    /// @param destID       node id of destination node 
    /// @param msg          msg to send
    /// @param ct           compute thread context
    void send(uint64_t destID, Message &msg, CT &ct) {
        // build rdma_ptr to the destID mailbox slot in mailboxes 
        auto mailptr = remus::rdma_ptr<Message>(
            mailboxes.id(), mailboxes.address() + destID * sizeof(Message)
        );

        // write msg to that mailbox slot
        ct->Write(mailptr, msg);
    }  

    /// poll response map until response with expected reqID arrives 
    /// @param reqid        request id to find
    Message waitfor(uint64_t reqid) {
        // wait for expected request id 
        while (true) {
            auto itr = resp_map.find(reqid); 
            if (itr != resp_map.end()) {
                Message msg = itr->second; 
                resp_map.erase(itr); 
                return msg; 
            }
        }
    }

    /// poll mailbox slot for this node and handle any message that arrives
    /// @param ct           compute thread context
    void pollMailbox(CT &ct) {
        // this node's slot is at index thisID
        auto thisMail = remus::rdma_ptr<Message>(
            mailboxes.id(), mailboxes.address() + thisID * sizeof(Message)
        );

        // continuous poll for any message 
        while (true) {
            Message msg = ct->Read(thisMail); 
            if (/*msg exists?*/) {
                switch(msg.type) {
                    case READ_REQ: 
                        handle_read_req(msg, ct); 
                        break; 
                    case READ_RES: 
                        handle_read_res(msg, ct); 
                        break; 
                }
            }
        }
    }



    ///     working with the cache 

    /// checks if the addr is cached locally
    /// @param addr     addr to look for
    /// @param out      where to store the location
    /// @return true if addr is cached locally, false otherwise
    bool cache_lookup(uint64_t addr, CacheLine &cline) {
        std::lock_guard<std::mutex> guard(mtx_lock); 
        auto itr = cache_map.find(addr); 
        if (itr != cache_map.end()) {
            cline = itr->second; 
            return true; 
        }
        return false; 
    }

    /// insert cache line into cache map 
    /// @param addr     the addr to add to
    /// @param line     the cache line to add
    void cache_insert(uint64_t addr, CacheLine &cline) {
        std::lock_guard<std::mutex> guard(mtx_lock); 
        cache_map[addr] = cline; 
    }

    /// sets the state of the cache line to invalid 
    /// @param addr     the addr of the cache line to invalidate 
    void cache_invalidate(uint64_t addr) {
        std::lock_guard<std::mutex> guard(mtx_lock); 
        auto itr = cache_map.find(addr); 
        if (itr != cache_map.end())
            itr->second.flag = INVALID; 
    }

    ///         reads 

    /// read the data at the addr
    /// @param ptr      rdma_ptr to the data entry to read from 
    /// @param ct       compute thread context
    /// @return the data read from the addr 
    uint64_t read(remus::rdma_ptr<DataEntry> ptr, CT &ct) {
        if (ptr.id() == thisID)
            return local_read(ptr, ct);
        else
            return remote_read(ptr, ct); 
    }

    /// local read -- request node is the home node 
    /// @param ptr      rdma_ptr to the data entry to read from 
    /// @param ct       compute thread context 
    uint64_t local_read(remus::rdma_ptr<DataEntry> ptr, CT &ct) {
        // get the data entry from the ptr 
        DataEntry dataE = ct->Read(ptr); 

        if (dataE.dir.flag == SHARED || dataE.dir.flag == UNSHARED) {
        // SHARED/UNSHARED -- can return the data
            return dataE.data[0];
        } else {
        // DIRTY
            return -1; // not implemented yet
        }
    }

    /// remote read -- request node is not the home node
    /// @param ptr      rdma_ptr to the data entry to read from
    /// @param ct       compute thread context 
    uint64_t remote_read(remus::rdma_ptr<DataEntry> ptr, CT &ct) {
        CacheLine cline; 
        // look for the address in the cache
        if (cache_lookup(ptr.address(), cline) && cline.flag != INVALID) {      // if found and not invalid 
            // return the cached data directly 
            return cline.data[0]; 
        } else {                // doesn't exist in the cache -- need to request from home node and store
            // send read request to home node
            Message msg; 
            msg.type = READ_REQ; 
            msg.addr = ptr.raw(); 
            msg.src = thisID; 
            msg.reqID = getReqID(); 

            send(ptr.id(), msg, ct); 

            // wait for response from home node 
            Message res = waitfor(msg.reqID, ct); 

            // insert into this node's cache 
            cline.flag = SHARED; 
            cline.home_node = ptr.id(); 
            memcpy(cline.data, res.data, sizeof(res.data)); 

            cache_insert(ptr.raw(), cline); 

            // return data 
            return cline.data[0];
        }
    }


    ///         writes 

    /// write data to the addr 
    /// @param ptr      rdma_ptr to write the data entry to
    /// @param val      data to write 
    /// @param ct       compute thread context
    void write(remus::rdma_ptr<DataEntry> ptr, uint64_t val, CT &ct); 
}