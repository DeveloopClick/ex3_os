//
// Created by T8832181 on 16/05/2023.
//

#ifndef _JOB_CONTEXT_H_
#define _JOB_CONTEXT_H_

#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include <atomic>
#include <vector>
#include <cstdint>

//class JobContext
//{
//
//  struct BitPartition
//  {
//      std::uint64_t state: 2;
//      std::uint64_t processed_keys: 31;
//      std::uint64_t total_keys: 31;
//  };
//  const InputVec &inputVec;
//  const MapReduceClient &client;
//  OutputVec &outputVec;
//  std::vector<IntermediateVec> our_queue;
//  IntermediateVec *thread_intermediate_vecs;
//  std::atomic<BitPartition> multi_purpose_counter;
//  int thread_index;
//  int num_of_vecs;
//  stage_t state;
//  JobContext (const MapReduceClient &client,
//              const InputVec &inputVec, OutputVec &outputVec,
//              int multiThreadLevel);
//  ~JobContext ();
//
//};
#endif //_JOB_CONTEXT_H_
