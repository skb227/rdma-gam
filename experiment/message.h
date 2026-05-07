#pragma once



#include <remus/remus.h> 

/// message types and communication channels 

enum MsgType {
    READ_REQ, 
    READ_RES
};

// data sent in a message 
struct Message {
    MsgType type; 
    uint64_t raw; 
    uint64_t srcID; 
    uint64_t reqID; 
    uint64_t data[64]; 
    bool valid = false;      // check that the message is valid 
};

// mailbox to receive requests 
struct Mailbox {
    Message mail[16];       // will have to update later so that each node doesn't have only one mailbox 
};