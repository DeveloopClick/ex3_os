#include "MapReduceFramework.h"
#include <cstdio>
#include <cstdlib>
#include <algorithm>
#include "JobContext.h"
#include <cmath>
#include <pthread.h>
#include <iostream>

void *run_job (void *wrapped_context);

int find_max_index (JobContext *context);
JobHandle startMapReduceJob (const MapReduceClient &client,
                             const InputVec &inputVec, OutputVec &outputVec,
                             int multiThreadLevel)
{
  if (multiThreadLevel < 1)
  {
    std::cout << "Error: multithread level " << multiThreadLevel << " is "
                                                                    "illegal\n"
              << std::endl;
    exit (1);
  }
  std::cout << "before-new-context"<< std::endl;
  auto *context = new JobContext (client, inputVec, outputVec, multiThreadLevel);
  std::cout << "after-new-context"<< std::endl;
  context->threads_list.reserve (multiThreadLevel);
  context->context_vec.reserve (multiThreadLevel);
  for (int i = 0; i < multiThreadLevel; i++)
  {
    context->context_vec[i] = {context, i};
  }
  std::cout << "map_stage"<< std::endl;

  context->state = {MAP_STAGE, 0};

  for (int i = 0; i < multiThreadLevel; i++)
  {
    if (0 != pthread_create (&context->threads_list[i], nullptr, run_job,
                             (&context->context_vec[i])))
    {
      std::cout << "Error: Failure to spawn new thread in run." << std::endl;
      exit (1);
    }
  }
  std::cout << "finished_creating"<< std::endl;

  return context;
}

void getJobState (JobHandle job, JobState *state)
{
  auto context = static_cast<JobContext *>(job);
  if (pthread_mutex_lock (&(context->jobStateMutex))
      != 0)
  {
    std::cout << "Error: Failure to lock the mutex in 1.\n" << std::endl;
    exit (1);
  }
  state->stage = context->state.stage;
  state->percentage = context->state.percentage;
  if (pthread_mutex_unlock (&(context->jobStateMutex))
      != 0)
  {
    std::cout << "Error: Failure to unlock the mutex in 2.\n" << std::endl;
    exit (1);
  }
}

void closeJobHandle (JobHandle job) // to fill?
{
  waitForJob (job);
  auto context = static_cast<JobContext *>(job);
//  if (context->closed_atomic_counter
//      != context->num_of_threads)
//  {
//    waitForJob (job);
//  }
  delete context;

}

void waitForJob (JobHandle job)
{
  auto context = static_cast<JobContext *>(job);
  if (!context->flag_waited.test_and_set ())
  {
    for (int i = 0; i < context->num_of_threads; i++)
    {
      if (pthread_join (context->threads_list[i], nullptr) != 0)
      {
        std::cout << "Error: Failure to join threads in run." << std::endl;
        exit (1);
      }
    }
  }
}

void emit2 (K2 *key, V2 *value, void *context)
{
  auto casted_context = static_cast<WrappedContext *>(context)->context;
  auto pair = IntermediatePair (key, value);
  auto index = static_cast<WrappedContext *>(context)->current_thread_ind;
  if (pthread_mutex_lock (&(casted_context->emit2Mutex)) != 0)
  {
    std::cout << "Error: Failure to unlock the mutex in." << std::endl;
    exit (1);
  }
  casted_context->thread_intermediate_vecs[index].push_back (pair);
  if (pthread_mutex_unlock (&(casted_context->emit2Mutex)) != 0)
  {
    std::cout << "Error: Failure to unlock the mutex in." << std::endl;
    exit (1);
  }

}

void emit3 (K3 *key, V3 *value, void *context)
{
  std::cout << "start emit3" << std::endl;
  auto casted_context = static_cast<JobContext *>(context);
  auto pair = OutputPair (key, value);
  if (pthread_mutex_lock (&(casted_context->emit3Mutex)) != 0)
  {
    std::cout << "Error: Failure to unlock the mutex." << std::endl;
    exit (1);
  }
  casted_context->outputVec.push_back (pair);
  if (pthread_mutex_unlock (&(casted_context->emit3Mutex)) != 0)
  {
    std::cout << "Error: Failure to unlock the mutex." << std::endl;
    exit (1);
  }
  std::cout << "end emit3" << std::endl;
}

bool compare_k2 (std::pair<K2 *, V2 *> &pair1, std::pair<K2 *, V2 *> &pair2)
{
  return *pair1.first < *pair2.first;
}

void *run_job (void *wrapped_context)
{
  int thread_ind = static_cast<WrappedContext *> (wrapped_context)->current_thread_ind;
  JobContext *context = static_cast<WrappedContext *> (wrapped_context)->context;
//  std::cout << "map"<< std::endl;

  /**MAP**/
  unsigned long old_value;
  while (*(context->map_atomic_counter)
         < context->inputVec.size ())
  {
    old_value = (*(context->map_atomic_counter))++;
    if (old_value >= context->inputVec.size ())
    {
      break;
    }

    context->client.map (context->inputVec[old_value].first,
                         context->inputVec[old_value].second,
                         wrapped_context);
    if (pthread_mutex_lock (&(context->jobStateMutex))
        != 0)
    {
      std::cout << "Error: Failure to unlock the mutex.\n" << std::endl;
      exit (1);
    }
    context->state.percentage = std::min (float (100), context->state
                                                           .percentage + 100
                                                                         * (1
                                                                            / float (context->inputVec
                                                                                         .size ())));
    if (pthread_mutex_unlock (&(context->jobStateMutex))
        != 0)
    {
      std::cout << "Error: Failure to unlock the mutex." << std::endl;
      exit (1);
    }
  }

  /**SORT**/
  if (!context->thread_intermediate_vecs[thread_ind].empty ())
  {

// TODO: make sure this doesnt mutex
//    auto &vec_to_sort = context->thread_intermediate_vecs[thread_ind];
    std::sort (context->thread_intermediate_vecs[thread_ind].begin (),
               context->thread_intermediate_vecs[thread_ind].end (), compare_k2);

  }
  /**BARRIER**/
  context->barrier->barrier ();

  /**SHUFFLE**/

  if (!context->flag_shuffle.test_and_set ())
  {
    if (pthread_mutex_lock (&(context->jobStateMutex))
        != 0)
    {
      std::cout << "Error: Failure to unlock the mutex in 13.\n" << std::endl;
      exit (1);
    }
    context->state = {SHUFFLE_STAGE, 0};
    if (pthread_mutex_unlock (&(context->jobStateMutex))
        != 0)
    {
      std::cout << "Error: Failure to unlock the mutex in 13.\n" << std::endl;
      exit (1);
    }
    for (int i = 0; i < context->thread_intermediate_vecs.size(); ++i)
    {
//      std::cout << i << " " << context->thread_intermediate_vecs[i].size()
//      << " ";
      if (context->thread_intermediate_vecs[i].empty ())
      {
        context->thread_intermediate_vecs.erase
            (context->thread_intermediate_vecs.begin () + i);
        i--;
      }
      else
      {
        context->num_of_intermediate_pairs += context->thread_intermediate_vecs[i].size ();
      }
    }
    while (!context->thread_intermediate_vecs.empty ())
    {
      int max_ind = find_max_index (context);
      auto vec_of_max_pair = &context->thread_intermediate_vecs[max_ind];

      IntermediateVec s_vec = IntermediateVec ();
//      std::cout << vec_of_max_pair->back ().first << std::endl;
      s_vec.push_back (vec_of_max_pair->back ());

      if (pthread_mutex_lock (&(context->jobStateMutex))
          != 0)
      {
        std::cout << "Error: Failure to unlock the mutex in 13."
                  << std::endl;
        exit (1);
      }
      context->state.percentage = std::min (float (100), context->state
                                                             .percentage
                                                         + 100 * (1 /
                                                                  float
                                                                      (context->num_of_intermediate_pairs)));
      if (pthread_mutex_unlock (&(context->jobStateMutex))
          != 0)
      {
        std::cout << "Error: Failure to unlock the mutex in 13.\n"
                  << std::endl;
        exit (1);
      }
      vec_of_max_pair->pop_back ();
      if (vec_of_max_pair->empty ())
      {
        context->thread_intermediate_vecs.erase
            (context->thread_intermediate_vecs.begin () + max_ind);

      }

      for (size_t i = 0; i < context->thread_intermediate_vecs.size (); ++i)
      {
        auto vec = &context->thread_intermediate_vecs[i];
        while (!vec->empty () && !((*s_vec.back ().first < *vec->back ().first)
                                   || (*vec->back ().first <
                                       *s_vec.back ().first)))
        {
//          std::cout << vec_of_max_pair->back ().first << std::endl;
          s_vec.push_back (vec->back ());
          if (pthread_mutex_lock (&(context->jobStateMutex))
              != 0)
          {
            std::cout << "Error: Failure to unlock the mutex in 13.\n"
                      << std::endl;
            exit (1);
          }
          context->state.percentage = std::min (float (100), context->state
                                                                 .percentage
                                                             + 100 * (1 /
                                                                      float
                                                                          (context->num_of_intermediate_pairs)));
          if (pthread_mutex_unlock (&(context->jobStateMutex))
              != 0)
          {
            std::cout << "Error: Failure to unlock the mutex in 13.\n"
                      << std::endl;
            exit (1);
          }

          vec->pop_back ();
        }
        if (vec->empty ())
        {
          //remove from thread_intermediate_vecs the current ind
          context->thread_intermediate_vecs.erase
              (context->thread_intermediate_vecs.begin () + i);
          i--;
        }
      }
      context->our_queue.push_back (s_vec);
    }

    if (pthread_mutex_lock (&(context->jobStateMutex))
        != 0)
    {
      std::cout << "Error: Failure to unlock the mutex in 13.\n" << std::endl;
      exit (1);
    }
    context->state = {REDUCE_STAGE, 0};
    std::cout << "finished shuffle?"<< std::endl;

    if (pthread_mutex_unlock (&(context->jobStateMutex))
        != 0)
    {
      std::cout << "Error: Failure to unlock the mutex in 13.\n" << std::endl;
      exit (1);
    }
    std::cout << "finished shuffle!"<< std::endl;
  }

  /**BARRIER**/
//  std::cout << "before-reduce-barrier"<< std::endl;
  context->barrier->barrier ();
//  std::cout << "after-reduce-barrier"<< std::endl;

  /**REDUCE**/
  std::cout << "reduce"<< std::endl;
  old_value = 0;
  // TODO: check condition for racing
  std::cout << *(context->reduce_atomic_counter) << " " <<
  context->our_queue.size () << std::endl;
  while (*(context->reduce_atomic_counter)
         < context->our_queue.size ())
  {
    old_value = (*(context->reduce_atomic_counter))++;
    if (old_value >= context->our_queue.size ())
    {
      break;
    }
    context->client.reduce (&context->our_queue[old_value],
                            context);
    std::cout << "tttttt"<< std::endl;

    if (pthread_mutex_lock (&(context->jobStateMutex)) != 0)
    {
      std::cout << "Error: Failure to unlock the mutex in 17.\n" << std::endl;
      exit (1);
    }
    context->state.percentage = std::ceil (1000 * std::min (float (100),
                                                            context->state.percentage
                                                            +
                                                            100
                                                            * (float (context->our_queue[old_value].size
                                                                ())
                                                               / float (context->num_of_intermediate_pairs))))
                                / 1000;
    if (pthread_mutex_unlock (&(context->jobStateMutex)) != 0)
    {
      std::cout << "Error: Failure to unlock the mutex in 18.\n" << std::endl;
      exit (1);
    }
  }
  std::cout << "yo"<< std::endl;

  return nullptr;
}
int find_max_index (JobContext *context)
{
  int max_ind = 0;
  auto vec_of_max_pair = &context->thread_intermediate_vecs[0];
  for (int i = 1; i < context->thread_intermediate_vecs.size (); ++i)
  {
    auto vec = &context->thread_intermediate_vecs[i];
    if (!vec->empty ()
        && *(vec_of_max_pair->back ().first) < *(vec->back ()
            .first))
    {
      vec_of_max_pair = vec;
      max_ind = i;
    }
  }
  return max_ind;
}



