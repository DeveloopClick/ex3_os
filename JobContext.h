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

struct WrappedContext;

class JobContext
{
 public:

  std::atomic<unsigned long> map_atomic_counter;
  std::atomic<unsigned long> shuffle_atomic_counter;
  std::atomic<unsigned long> reduce_atomic_counter;
  std::atomic<unsigned long> closed_atomic_counter;


  const MapReduceClient &client;
  const InputVec &inputVec;
  OutputVec &outputVec;

  std::vector<IntermediateVec> our_queue;
  std::vector<IntermediateVec> thread_intermediate_vecs;
  std::vector<pthread_t> threads_list;
  std::vector<WrappedContext> context_vec;

  int num_of_threads;
  int num_of_intermediate_pairs;

  std::atomic_flag flag_waited;
  std::atomic_flag flag_map;
  std::atomic_flag flag_shuffle;
  std::atomic_flag flag_reduce;

  JobState state;
  pthread_mutex_t emit3Mutex;
  pthread_mutex_t jobStateMutex;
  Barrier *barrier;

  JobContext (const MapReduceClient &client,
              const InputVec &inputVec, OutputVec &outputVec,
              int multiThreadLevel);
  ~JobContext ();

};

struct WrappedContext
{
    JobContext *context;
    int current_thread_ind;
};

#endif //_JOB_CONTEXT_H_
