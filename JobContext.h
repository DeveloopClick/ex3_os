//
// Created by T8832181 on 16/05/2023.
//

#ifndef _JOB_CONTEXT_H_
#define _JOB_CONTEXT_H_

#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include "Barrier/Barrier.h"
#include <atomic>
#include <vector>
#include <cstdio>
#include <cstdlib>
#include <pthread.h>

class JobContext
{
 public:

  std::atomic<unsigned long> map_atomic_counter;
//  std::atomic<unsigned long> shuffle_atomic_counter;
  std::atomic<unsigned long> reduce_atomic_counter;

  const InputVec &inputVec;
  const MapReduceClient &client;
  OutputVec &outputVec;
  std::vector<IntermediateVec> our_queue;
  IntermediateVec *thread_intermediate_vecs;
  int thread_index;
  int num_of_intermediate_vecs;
  JobState state;
  pthread_mutex_t inVecMutex;
  pthread_mutex_t shuffleMutex;
  pthread_mutex_t outVecMutex;
  int first_to_map;
  int first_to_shuffle;
  int first_to_reduce;
  Barrier barrier;
  JobContext (const MapReduceClient &client,
              const InputVec &inputVec, OutputVec &outputVec,
              int multiThreadLevel);
  ~JobContext ();

};
#endif //_JOB_CONTEXT_H_
