#include <memory>
#include <unistd.h>
#include <vector>

#include <iostream>

#include <remus/remus.h>

#include "cloudlab.h"
#include "nodes.h"
#include "params.h"
#include "workload.h"

int main (int argc, char **argv) {

    // initialize remus
    remus::INIT(); 

    // configure and parse arguments
    auto args = std::make_shared<remus::ArgMap>(); 
    args->import(remus::ARGS);
    args->import(DS_EXP_ARGS);
    args->parse(argc, argv);

    // extract args needed in every node 
    uint64_t id = args->uget(remus::NODE_ID);
    uint64_t m0 = args->uget(remus::FIRST_MN_ID); 
    uint64_t mn = args->uget(remus::LAST_MN_ID);
    uint64_t c0 = args->uget(remus::FIRST_CN_ID);
    uint64_t cn = args->uget(remus::LAST_CN_ID);

    // prepare network info about this machine and about memory nodes
    remus::MachineInfo self(id, id_to_dns_name(id)); 
    std::vector<remus::MachineInfo> memnodes; 
    for (uint64_t i = m0; i <= mn; ++i) {
        memnodes.emplace_back(i, id_to_dns_name(i)); 
    }

    // info for memory node
    std::unique_ptr<remus::MemoryNode> memory_node; 

    // info for compute node
    std::shared_ptr<remus::ComputeNode> compute_node;

    // if memory node, configure to be memory node
    if (id >= m0 && id <= mn) {
        // make the pools, await connections 
        memory_node.reset(new remus::MemoryNode(self, args));
    }

    // if compute node
    if (id >= c0 && id <= cn) {
        compute_node.reset(new remus::ComputeNode(self, args)); 
        // if this CN is also a MN, then need to pass the rkeys to the local MN
        //  there's no harm in doing them first
        if (memory_node.get() != nullptr) {
            auto rkeys = memory_node->get_local_rkeys(); 
            compute_node->connect_local(memnodes, rkeys);
        }
        compute_node->connect_remote(memnodes);
    }

    // if memory node, pause until it's received all expected connections
    //      then spin until control channel in each segment is 1
    //      then shutdown memory node
    if (memory_node) {
        memory_node->init_done(); 
    }

    // if compute node, create threads and run experiment 
    if (id >= c0 && id <= cn) {
        // create ComputeThread contexts
        std::vector<std::shared_ptr<remus::ComputeThread>> compute_threads; 
        uint64_t total_threads = (cn - c0 + 1) * args->uget(remus::CN_THREADS); 
        for (uint64_t i = 0; i < args->uget(remus::CN_THREADS); ++i) {
            compute_threads.push_back(std::make_shared<remus::ComputeThread>(id, compute_node, args)); 
        }

        // CN 0 will construct the data structure and save it in root
        if (id == c0) {
            auto set_ptr = LazyListSet::New(compute_threads[0]);
            compute_threads[0]->set_root(set_ptr); 
        }

        // make threads and start them
        std::vector<std::thread> worker_threads; 
        for (uint64_t i = 0; i < args->uget(remus::CN_THREADS); i++) {
            worker_threads.push_back(std::thread(
                [&](uint64_t i) {
                    // each node has its own compute thread context 
                    auto &ct = compute_threads[i]; 
                    // wait for all threads to be created across all nodes
                    ct->arrive_control_barrier(total_threads);

                    // std::cout << "past barrier 1" << std::endl; 

                    // get the root, make a local reference to it
                    auto set_ptr = ct->get_root<LazyListSet>();
                    // call constructor for LazyListSet
                    LazyListSet set_handle(set_ptr);


                    // std::cout << "workload should go here" << std::endl; 

        // workload test

                    // make workload manager for this thread 
                    test workload(set_handle, i, id); 
                    ct->arrive_control_barrier(total_threads); 

                    // std::cout << "past barrier 2" << std::endl; 

                    // prefill data structure
                    workload.prefill(ct, args); 
                    ct->arrive_control_barrier(total_threads); 

                    // std::cout << "past barrier 3" << std::endl; 

                    // get starting time before thread does any work 
                    std::chrono::high_resolution_clock::time_point start_time = std::chrono::high_resolution_clock::now(); 
                    ct->arrive_control_barrier(total_threads); 

                    // std::cout << "past barrier 4" << std::endl; 

                    // std::cout << "about to run workload" << std::endl; 

                    // run workload 
                    workload.run(ct, args); 
                    ct->arrive_control_barrier(total_threads); 

                    // std::cout << "past barrier 5" << std::endl; 

                    // compute end time 
                    auto end_time = std::chrono::high_resolution_clock::now(); 
                    auto duration = std::chrono::duration_cast<std::chrono::microseconds>( end_time - start_time ).count(); 

        // end of workload test
                
                    // wait till all threads finish workload test
                    // ct->arrive_control_barrier(total_threads); 

                    // reclaim memory from prior phase
                    ct->ReclaimDeferred(); 

                    // std::cout << "compile metrics objects" << std::endl; 

                    // first thread of cn0 works with data structure, creates global metrics object
                    if (id == c0 && i == 0) {
                        set_handle.destroy(ct); 
                        auto metrics = ct->allocate<Metrics>(); 
                        ct->Write(metrics, Metrics()); 
                        ct->set_root(metrics); 
                    }
                    ct->arrive_control_barrier(total_threads); 

                    // std::cout << "past barrier six" << std::endl; 

                    // aggregate metrics across all threads
                    auto metrics = remus::rdma_ptr<Metrics>(ct->get_root<Metrics>());
                    workload.collect(ct, metrics); 
                    ct->arrive_control_barrier(total_threads); 

                    // std::cout << "past barrier seven" << std::endl; 

                    // first thread of cn0 write aggregate metrics to file
                    if (id == c0 && i == 0) {
                        compute_threads[0]->Read(metrics).to_file(duration, compute_threads[0]); 
                    }

                    // std::cout << "written to file " << std::endl; 
                },
            i));
        }
        for (auto &t : worker_threads) {
            t.join(); 
        }
    } 
};
