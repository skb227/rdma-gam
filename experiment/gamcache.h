#pragma once

#include <unordered_map>
#include <mutex>
#include <remus/remus.h>
#include "components.h"
#include "message.h"

using CT = std::shared_ptr<remus::ComputeThread>; 

class GAMcache {

    std::unordered_map<uint64_t, CacheLine> cache_map; 
    std::mutex mtx_lock;            // for accessing cache_map
    uint64_t thisID;                // this node's id

    // for mailbox and requests
    remus::rdma_ptr<Message> mailboxes; 
    std::atomic<uint64_t> nextReqID{0}; 

    // for incoming responses 
    std::unordered_map<uint64_t, Message> resp_map; 
    std::mutex mtx_resp; 


public: 
    
    GAMcache(uint64_t node_id, remus::rdma_ptr<Message> mbox) 
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

        // ensure valid bit is set
        msg.valid = true; 

        std::cout << "send is writing with req id " << msg.reqID << std::endl; 

        // write msg to that mailbox slot
        ct->Write(mailptr, msg);
    }  

    /// poll response map until response with expected reqID arrives 
    /// @param reqid        request id to find
    Message waitfor(uint64_t reqid) {
        // wait for expected request id 
        std::cout << "waitfor looking for reqid " << reqid << std::endl; 
        while (true) {
            {    
                // get lock on the res map 
                std::lock_guard<std::mutex> guard(mtx_resp); 
                auto itr = resp_map.find(reqid); 
                if (itr != resp_map.end()) {
                    std::cout << "message found" << std::endl; 
                    Message msg = itr->second; 
                    resp_map.erase(itr); 
                    return msg; 
                }
            }
            std::this_thread::yield();              // can switch to another thread if available, but don't lose this thread 
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
            if (msg.valid) {
                std::cout << "found a valid message in polling thread" << std::endl; 
                switch(msg.type) {
                    case READ_REQ: 
                        handle_read_req(msg, ct); 
                        msg.valid = false; ct->Write(thisMail, msg); 
                        break; 
                    case READ_RES: 
                        handle_read_res(msg, ct); 
                        msg.valid = false; ct->Write(thisMail, msg); 
                        break; 
                }
            }
        }
    }


    ///     handling message requests 

    /// handle read requests
    /// @param msg      msg that was sent 
    /// @param ct       compute thread context
    void handle_read_req(Message msg, CT &ct) {
        std::cout << "handle read req" << std::endl; 
        // so handling read request would occur on the home node -- meaning i have access to the directory entry 
            // might be smart to add a safety check anyways 

        // build the rdma_ptr to DataEntry with the msg.raw address 
        auto ptr = remus::rdma_ptr<DataEntry> (msg.raw);   // can i send in the entire raw address or do i need to do .id then .address? 

        // read the DataEntry from ptr
        DataEntry dataE = ct->Read(ptr); 

        // check the metadata flag in data entry 
        if (dataE.dir.flag == SHARED || dataE.dir.flag == UNSHARED) { 
            // build a message to send back to request node 
            Message newmsg; 
            newmsg.type = READ_RES; 
            memcpy(newmsg.data, dataE.data, sizeof(dataE.data)); 
            newmsg.raw =  msg.raw; 
            newmsg.srcID = thisID; 
            newmsg.reqID = msg.reqID;               // use the same request id 
            newmsg.valid = true; 

            // send the new message to the request node's inbox 
            std::cout << "sending response to " << msg.srcID << std::endl; 
            send(msg.srcID, newmsg, ct); 

            // update directory entry 
            dataE.dir.flag = SHARED; 
            dataE.dir.slist_add(msg.srcID);     // add request node id to share list  

            // write the updated directory 
            ct->Write(ptr, dataE); 
        } else {                                    // has a dirty owner node 
            // to build later 
        }
    }

    /// handle read responses 
    /// @param msg      msg received
    /// @param ct       compute thread context 
    void handle_read_res(Message msg, CT &ct) {
        // would be in the request node here 
                // again might be smart to add a safety check 
        
        // lock the response map 
        std::lock_guard<std::mutex> guard(mtx_resp); 
        CT ct2 = ct; // just to get rid of compiler complaint about unused parameter
        
        // write the msg to the resp map 

        std::cout << "write response to " << msg.reqID << std::endl; 
        resp_map[msg.reqID] = msg;
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
        std::cout << "is the segmentation fault in the Read(ptr)?" << std::endl; 
        std::cout << ptr.id() << ", " << ptr.address() << std::endl; 
        DataEntry dataE = ct->Read(ptr); 
        std::cout << "in read" << std::endl; 
        std::cout << "comparing home node id " << dataE.homeNode << " with thisID " << thisID << std::endl; 
        if (dataE.homeNode == thisID)
            return local_read(ptr, dataE, ct);
        else
            return remote_read(ptr, dataE, ct); 
    }

    /// local read -- request node is the home node 
    /// @param ptr      rdma_ptr to the data entry to read from 
    /// @param dataE    data entry to home node 
    /// @param ct       compute thread context 
    uint64_t local_read(remus::rdma_ptr<DataEntry> ptr, DataEntry dataE, CT &ct) {
        std::cout << "in local read" << std::endl; 

        if (dataE.dir.flag == SHARED || dataE.dir.flag == UNSHARED) {
        // SHARED/UNSHARED -- can return the data
            return dataE.data[0];
        } else {
        // DIRTY
            DataEntry rmv = ct->Read(ptr); 
            std::cout << "make compiler go away" << rmv.dir.flag << std::endl; 
            return -1; // not implemented yet
        }
    }

    /// remote read -- request node is not the home node
    /// @param ptr      rdma_ptr to the data entry to read from
    /// @param dataE    data entry from home node 
    /// @param ct       compute thread context 
    uint64_t remote_read(remus::rdma_ptr<DataEntry> ptr, DataEntry dataE, CT &ct) {
        std::cout << "in remote read" << std::endl; 
        CacheLine cline; 
        // look for the address in the cache
        if (cache_lookup(ptr.address(), cline) && cline.flag != INVALID) {      // if found and not invalid 
            std::cout << "cache hit" << std::endl; 
            // return the cached data directly 
            return cline.data[0]; 
        } else {                // doesn't exist in the cache -- need to request from home node and store
            std::cout << "cache miss" << std::endl; 
            // send read request to home node
            Message msg; 
            msg.type = READ_REQ; 
            msg.raw = ptr.raw(); 
            msg.srcID = thisID; 
            msg.reqID = getReqID(); 
            msg.valid = true; 

            std::cout << "build the message" << std::endl; 
            std::cout << "sending message" << std::endl; 

            send(dataE.homeNode, msg, ct); 

            std::cout << "message sent, waiting for response" << std::endl; 

            // wait for response from home node 
            Message res = waitfor(msg.reqID); 

            std::cout << "response received" << std::endl; 

            // insert into this node's cache 
            cline.flag = SHARED; 
            cline.home_node = ptr.id(); 
            memcpy(cline.data, res.data, sizeof(res.data)); 

            std::cout << "inserting into cache" << std::endl; 

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
};