#include <pthread.h>
#include "MapReduceFramework.h"
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <algorithm>
#include "Barrier/Barrier.h"
#include "JobContext.h"

pthread_t *threads_list;
int flag_wait=0;

struct WrappedContext
{
    JobContext *context;
    int current_thread_ind;
};

JobHandle startMapReduceJob (const MapReduceClient &client,
                             const InputVec &inputVec, OutputVec &outputVec,
                             int multiThreadLevel)
{

//  if (multiThreadLevel < 1)
//  {
//    fprintf (stderr, "Error: multithread level %d is illegal.\n", multiThreadLevel);
//    exit (1);
//  }

  JobContext *context=new JobContext(client,inputVec,outputVec,multiThreadLevel);

  threads_list = new pthread_t[multiThreadLevel];

  WrappedContext context_vec[multiThreadLevel];
  for (int i = 0; i < multiThreadLevel; i++)
  {
    context_vec[i] = WrappedContext{context, i};
  }

  for (int i = 1; i < multiThreadLevel; i++)
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
  JobState current_state = static_cast<JobContext *>(job)->state;
  state->stage = current_state.stage;
}
void closeJobHandle (JobHandle job)
{
  auto context = static_cast<JobContext *>(job);
  delete context;
  delete [] threads_list;
}


void waitForJob (JobHandle job)
{
  if (flag_wait==1)
  {
    return;
  }
  auto context = static_cast<JobContext *>(job);
  for (int i = 1; i < context->num_of_intermediate_vecs; i++)
  {
    if (0 != pthread_join (threads_list[i], NULL))
    {
      fprintf (stderr, "Error: Failure to join threads in run.\n");
      exit (1);
    }
  }
  flag_wait=1;
}

void emit2 (K2 *key, V2 *value, void *context)
{
  auto casted_context = static_cast<JobContext *>(context);
  auto pair = IntermediatePair (key, value);
  auto index = casted_context->thread_index;
//     no need for mutex because each thread access it's own vector and only
//  one thread can access it
  casted_context->thread_intermediate_vecs[index].push_back (pair);
}

void emit3 (K3 *key, V3 *value, void *context)
{
  auto casted_context = static_cast<JobContext *>(context);
  auto pair = OutputPair (key, value);
  if (pthread_mutex_lock (&(casted_context->outVecMutex)) != 0)
  {
    fprintf (stderr, "Error: Failure to lock the mutex in emit3.\n");
    exit (1);
  }
  casted_context->outputVec.push_back (pair);
  if (pthread_mutex_unlock (&(casted_context->outVecMutex)) != 0)
  {
    fprintf (stderr, "Error: Failure to unlock the mutex in emit3.\n");
    exit (1);
  }
}

bool compare_k2(std::pair<K2 *, V2 *> &pair1,std::pair<K2 *, V2 *> &pair2)
//check types
{
  return pair1.first > pair2.first;
}

void *run_job (WrappedContext *wrapped_context)
{

  /************************************************
   *                  MAP PHASE                   *
   ************************************************/
  int thread_ind = static_cast<WrappedContext *> (wrapped_context)->current_thread_ind;
  JobContext *context = static_cast<WrappedContext *> (wrapped_context)->context;

  context->state.stage = MAP_STAGE;

  unsigned long old_value = 0;

  while ((old_value = context->map_atomic_counter++)
         < context->inputVec.size ())
  {
    if (pthread_mutex_lock (&(context->inVecMutex)) != 0)
    {
      fprintf (stderr, "Error: Failure to lock the mutex in emit3.\n");
      exit (1);
    }
    context->client.map (context->inputVec[old_value].first,
                         context->inputVec[old_value].second,
                         context);
    if (pthread_mutex_unlock (&(context->inVecMutex)) != 0)
    {
      fprintf (stderr, "Error: Failure to unlock the mutex in emit3.\n");
      exit (1);
    }
  }
  //emits to emit2 by itself, only implement emit2, so it'll work and update
  // the output vec (intermediate vector)

  /************************************************
   *                  SORT PHASE                   *
   ************************************************/


  if (!context->thread_intermediate_vecs[thread_ind].empty ())
  {
    std::sort (context->thread_intermediate_vecs[thread_ind].begin (),
               context->thread_intermediate_vecs[thread_ind].end (), compare_k2);

  }

  /************************************************
   *                  BARRIER                     *
   ************************************************/

  // Barrier for all threads
  context->barrier.barrier();

  /************************************************
   *              SHUFFLE \ REDUCE                *
   ************************************************/

  /************************************************
   *                  SHUFFLE                     *
   ************************************************/
  // first to retrive 0 (atomically) is crowned shuffler. Long shall he reign!
  int first = context->shufflerRace++;
  if (first == 0)
  {

    // lock for the rest of the threads
    context->shuffleState = ShuffleState::IN_SHUFFLE;

  // MAKE SO ONLY ONE CONTINUES
  while(!thread_intermediate_vecs.empty())
  {
    int max_ind = 0;
    auto vec_of_max_pair = thread_intermediate_vecs[0];
    for (int i = 0; i < thread_intermediate_vecs.length(); ++i)
    {
      auto vec = thread_intermediate_vecs[i];
      if (vec.back() != NULL && vec_of_max_pair.back().first() < vec.back().first())
      {
        vec_of_max_pair = vec;
        max_ind = i;
      }
    }

    IntermediateVec s_vec = IntermediateVec();  // TODO: free
    vec.push_back(vec_of_max_pair.back());
    vec_of_max_pair.pop_back();
    if(vec_of_max_pair.empty())
    {
      //remove from thread_intermediate_vecs the ind of max_ind
    }

    for (int i = 0; i < thread_intermediate_vecs.length(); ++i)
    {
      auto vec = thread_intermediate_vecs[i];
      if (vec.back() != NULL && s_vec.back().first() == vec.back().first())
      {
        s_vec.push_back(vec.back());
        vec.pop_back();
        if(vec.empty())
        {
          //remove from thread_intermediate_vecs the current ind
        }
      }
    }
    our_queue.push_back(s_vec);
  }

  /************************************************
   *                  REDUCE                      *
   ************************************************/
  context->state.stage = REDUCE_STAGE;

  old_value = 0;

  while ((old_value = context->reduce_atomic_counter++)
         < context->our_queue.size())
  {
    if (pthread_mutex_lock (&(context->outVecMutex)) != 0)
    {
      fprintf (stderr, "Error: Failure to lock the mutex in emit3.\n");
      exit (1);
    }
    context->client.reduce(&context->our_queue[old_value],
                         context);
    if (pthread_mutex_unlock (&(context->inVecMutex)) != 0)
    {
      fprintf (stderr, "Error: Failure to unlock the mutex in emit3.\n");
      exit (1);
    }
  }
  // All threads continue here. ShuffleLocked represents the shuffler is still working
  unsigned long task_num = 0;
  while (true)
  {
    if (context->reduceTaskCounter >= context->uniqueK2Size)
    {
      break;
    }

    // Wait for the shuffler to populate queue. Signal comes through semaphore
    if (sem_wait (&context->taskCountSem) != ErrorCode::SUCCESS)
    {
      fprintf (stderr, "Error: REDUCER Semaphore failure in waiting thread.\n");
      exit (1);
    }

    // Lock the mutex to access mutual queue
    if (sem_wait (&context->taskQueueSem) != ErrorCode::SUCCESS)
    {
      fprintf (stderr, "Error: REDUCER Mutex lock failure in waiting thread.\n");
      exit (1);
    }

    // Leave if all tasks were performed
    task_num = context->reduceTaskCounter++;

    IntermediateVec job;
    // retrieve next job if exists
    if (task_num < context->uniqueK2Size)
    {
      job = context->readyQueue[task_num];

    }

    if (sem_post (&context->taskQueueSem) != ErrorCode::SUCCESS)
    {
      fprintf (stderr, "Error: REDUCER Mutex unlock failure in waiting thread.\n");
      exit (1);
    }

    if (task_num < context->uniqueK2Size)
    {
      context->client.reduce (&job, contextWrapper);
    }
  }
  return (void *) ErrorCode::SUCCESS;
}
}