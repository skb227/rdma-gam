#pragma once



#include <remus/remus.h> 

/// message types and communication channels 

enum MsgType {
    READ_REQ, 
    READ_RESP, 
    FETCH_REQ,
    FETCH_RESP
};

// data sent in a message 
struct Message {
    MsgType type; 
    uint64_t addr; 
    uint64_t src; 
    uint64_t reqID; 
    uint64_t data[64]; 
};

// mailbox to receive requests 
struct Mailbox {
    Message mail[16];       // will have to update later so that each node doesn't have only one mailbox 
}