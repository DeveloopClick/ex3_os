#include "MapReduceFramework.h"
#include <cstdio>
#include <cstdlib>
#include <algorithm>
#include "JobContext.h"
#include <cmath>
#include <pthread.h>
#include <iostream>
pthread_t *threads_list;

struct WrappedContext
{
    JobContext *context;
    int current_thread_ind;
};

void *run_job (void *wrapped_context);

JobHandle startMapReduceJob (const MapReduceClient &client,
                             const InputVec &inputVec, OutputVec &outputVec,
                             int multiThreadLevel)
{
  if (multiThreadLevel < 1)
  {
    fprintf (stderr, "Error: multithread level %d is illegal.\n", multiThreadLevel);
    exit (1);
  }

  auto *context = new JobContext (client, inputVec, outputVec, multiThreadLevel);
  threads_list = new pthread_t[multiThreadLevel];
  auto *context_vec = new WrappedContext[multiThreadLevel];

  for (int i = 0; i < multiThreadLevel; i++)
  {
    context_vec[i] = {context, i};
  }

  for (int i = 0; i < multiThreadLevel; i++)
  {
    if (0 != pthread_create (&threads_list[i], nullptr, run_job,
                             static_cast<void *>(&context_vec[i])))
    {
      fprintf (stderr, "Error: Failure to spawn new thread in run.\n");
      exit (1);
    }
  }
  return context;
}

void getJobState (JobHandle job, JobState *state)
{
  if (pthread_mutex_lock (&(static_cast<JobContext *>(job)->jobStateMutex))
      != 0)
  {
    fprintf (stderr, "Error: Failure to lock the mutex in emit3.\n");
    exit (1);
  }
  state->stage = static_cast<JobContext *>(job)->state.stage;
  state->percentage = static_cast<JobContext *>(job)->state.percentage;
  if (pthread_mutex_unlock (&(static_cast<JobContext *>(job)->jobStateMutex))
      != 0)
  {
    fprintf (stderr, "Error: Failure to unlock the mutex in emit3.\n");
    exit (1);
  }
}

void closeJobHandle (JobHandle job) // to fill?
{
  auto context=static_cast<JobContext *>(job);
  if (context->closed_atomic_counter
      != context->num_of_threads)
  {
    waitForJob (job);
  }
  delete context;
  delete[] threads_list;
}

void waitForJob (JobHandle job)
{
  auto context = static_cast<JobContext *>(job);

  if (context->flag_waited == 1)
  {
    return;
  }
  for (int i = 1; i < context->num_of_threads; i++)
  {
    if (pthread_join (threads_list[i], nullptr)!=0)
    {
      fprintf (stderr, "Error: Failure to join threads in run.\n");
      exit (1);
    }
  }
  context->flag_waited = 1;
}

void emit2 (K2 *key, V2 *value, void *context)
{
  auto casted_context = static_cast<JobContext *>(context);
  auto pair = IntermediatePair (key, value);
  auto index = casted_context->thread_index;
  if (pthread_mutex_lock (&(casted_context->emit2Mutex)) != 0)
  {
    fprintf (stderr, "Error: Failure to lock the mutex in emit3.\n");
    exit (1);
  }
  casted_context->thread_intermediate_vecs[index].push_back (pair);
  if (pthread_mutex_unlock (&(casted_context->emit2Mutex)) != 0)
  {
    fprintf (stderr, "Error: Failure to lock the mutex in emit3.\n");
    exit (1);
  }

}

void emit3 (K3 *key, V3 *value, void *context)
{
  auto casted_context = static_cast<JobContext *>(context);
  auto pair = OutputPair (key, value);
  if (pthread_mutex_lock (&(casted_context->emit3Mutex)) != 0)
  {
    fprintf (stderr, "Error: Failure to lock the mutex in emit3.\n");
    exit (1);
  }
  casted_context->outputVec.push_back (pair);
  if (pthread_mutex_unlock (&(casted_context->emit3Mutex)) != 0)
  {
    fprintf (stderr, "Error: Failure to unlock the mutex in emit3.\n");
    exit (1);
  }
}

bool compare_k2 (std::pair<K2 *, V2 *> &pair1, std::pair<K2 *, V2 *> &pair2)
{
  return *pair1.first < *pair2.first;
}

void *run_job (void *wrapped_context)
{

// TODO: why do wee need thread_ind and context->thread_index
  int thread_ind = static_cast<WrappedContext *> (wrapped_context)->current_thread_ind;
  JobContext *context = static_cast<WrappedContext *> (wrapped_context)->context;

  /**MAP**/
  if (pthread_mutex_lock (&(context->jobStateMutex))
      != 0)
  {
    fprintf (stderr, "Error: Failure to lock the mutex in emit3.\n");
    exit (1);
  }
  context->state.stage = MAP_STAGE;
  if (context->first_to_map++ == 0)
  {
    context->state.percentage = 0;
  }
  if (pthread_mutex_unlock (&(context->jobStateMutex))
      != 0)
  {
    fprintf (stderr, "Error: Failure to lock the mutex in emit3.\n");
    exit (1);
  }


  unsigned long old_value = 0;
  while ((old_value = context->map_atomic_counter++)
         < context->inputVec.size ())
  {

    if (pthread_mutex_lock (&(context->inVecMutex)) != 0)
    {
      fprintf (stderr, "Error: Failure to lock the mutex in emit3.\n");
      exit (1);
    }
    context->thread_index = thread_ind;
    context->client.map (context->inputVec[old_value].first,
                         context->inputVec[old_value].second,
                         context);
    context->state.percentage = std::min(float(100),context->state
    .percentage + 100 * (1 / float (context->inputVec
                                                       .size ())));
    context->thread_index = -1;
    if (pthread_mutex_unlock (&(context->inVecMutex)) != 0)
    {
      fprintf (stderr, "Error: Failure to unlock the mutex in emit3.\n");
      exit (1);
    }
  }

  /**SORT**/

  if (!context->thread_intermediate_vecs[thread_ind].empty ())
  {
    if (pthread_mutex_lock (&(context->sortMutex)) != 0)
    {
      fprintf (stderr, "Error: Failure to lock the mutex in emit3.\n");
      exit (1);
    }
    std::sort (context->thread_intermediate_vecs[thread_ind].begin (),
               context->thread_intermediate_vecs[thread_ind].end (), compare_k2);
    if (pthread_mutex_unlock (&(context->sortMutex)) != 0)
    {
      fprintf (stderr, "Error: Failure to lock the mutex in emit3.\n");
      exit (1);
    }
  }
//  usleep (10000);//TODO: delete!!!

  /**BARRIER**/
  context->barrier.barrier ();

  /**SHUFFLE**/
  if (pthread_mutex_lock (&(context->jobStateMutex))
      != 0)
  {
    fprintf (stderr, "Error: Failure to lock the mutex in emit3.\n");
    exit (1);
  }
  context->state.stage = SHUFFLE_STAGE;
  if (context->first_to_shuffle++ == 0)
  {
    context->state.percentage = 0;
  }
  if (pthread_mutex_unlock (&(context->jobStateMutex))
      != 0)
  {
    fprintf (stderr, "Error: Failure to lock the mutex in emit3.\n");
    exit (1);
  }

//  int first = context->first_to_shuffle++;
// decided by last
  if (pthread_mutex_lock (&context->shuffleMutex) != 0)
  {
    fprintf (stderr, "[[ShuffleBarrier]] error on pthread_mutex_lock");
    exit (1);
  }

  if (++context->shuffle_atomic_counter < context->num_of_threads)
  {
    if (pthread_cond_wait (&context->cv, &context->shuffleMutex) != 0)
    {
      fprintf (stderr, "[[ShuffleBarrier]] error on pthread_cond_wait");
      exit (1);
    }
  }
  else
  {
    context->shuffle_atomic_counter = 0;
    for (int i = 0; i < context->num_of_threads; ++i)
    {
      if (context->thread_intermediate_vecs[i].empty ())
      {
        context->thread_intermediate_vecs.erase
            (context->thread_intermediate_vecs.begin () + i);
      }
      else
      {

        context->num_of_intermediate_pairs += context->thread_intermediate_vecs[i].size ();
      }
    }
    // ONLY THE LAST THREAD CONTINUES AND SHUFFLES
    while (!context->thread_intermediate_vecs.empty ())
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

      IntermediateVec s_vec = IntermediateVec ();
//      std::cout << vec_of_max_pair->back ().first << std::endl;
      s_vec.push_back (vec_of_max_pair->back ());
      context->state.percentage =std::min(float(100),context->state
          .percentage + 100 *
          (1 /
                                          float
                                              (context->num_of_intermediate_pairs)));
      vec_of_max_pair->pop_back ();
      if (vec_of_max_pair->empty ())
      {
        context->thread_intermediate_vecs.erase
            (context->thread_intermediate_vecs.begin () + max_ind);

      }

      for (int i = 0; i < context->thread_intermediate_vecs.size (); ++i)
      {
        auto vec = &context->thread_intermediate_vecs[i];
        while (!vec->empty () && !((*s_vec.back ().first < *vec->back ().first)
                                || (*vec->back ().first <
                                    *s_vec.back ().first)))
        {
//          std::cout << vec_of_max_pair->back ().first << std::endl;
          s_vec.push_back (vec->back ());
          context->state.percentage = std::min(float(100),context->state
          .percentage +100 * (1 /
                                              float
                                              (context->num_of_intermediate_pairs)));

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
//      std::cout << "pushed" << std::endl;
//      usleep (10000); //TODO: delete!!
      context->our_queue.push_back (s_vec);
    }

    if (pthread_cond_broadcast (&context->cv) != 0)
    {
      fprintf (stderr, "[[ShuffleBarrier]] error on pthread_cond_broadcast");
      exit (1);
    }
  }
  if (pthread_mutex_unlock (&context->shuffleMutex) != 0)
  {
    fprintf (stderr, "[[ShuffleBarrier]] error on pthread_mutex_unlock");
    exit (1);
  }

  /**REDUCE**/
  if (pthread_mutex_lock (&(context->jobStateMutex))
      != 0)
  {
    fprintf (stderr, "Error: Failure to lock the mutex in emit3.\n");
    exit (1);
  }
  context->state.stage = REDUCE_STAGE;
  if (context->first_to_reduce++ == 0)
  {
    context->state.percentage = 0;
  }
  if (pthread_mutex_unlock (&(context->jobStateMutex))
      != 0)
  {
    fprintf (stderr, "Error: Failure to lock the mutex in emit3.\n");
    exit (1);
  }

  old_value = 0;
  while ((old_value = context->reduce_atomic_counter++)
         < context->our_queue.size ())
  {
    if (pthread_mutex_lock (&(context->outVecMutex)) != 0)
    {
      fprintf (stderr, "Error: Failure to lock the mutex in emit3.\n");
      exit (1);
    }
    context->client.reduce (&context->our_queue[old_value],
                            context);
    context->state.percentage =std::min(float(100),context->state.percentage +
        100 * (float (context->our_queue[old_value].size
            ())
               / float (context->num_of_intermediate_pairs)));
    if (pthread_mutex_unlock (&(context->outVecMutex)) != 0)
    {
      fprintf (stderr, "Error: Failure to unlock the mutex in emit3.\n");
      exit (1);
    }
  }
  //TODO: this is a plaster - remove!
//  context->state.percentage = std::round (context->state.percentage);
  context->closed_atomic_counter++;
  return nullptr;
}
