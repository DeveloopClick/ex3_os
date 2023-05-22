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
    context->client.map (context->inputVec[old_value].first,
                         context->inputVec[old_value].second,
                         context);

  }
  //emits to emit2 by itself, only implement emit2, so it'll work and update
  // the output vec (intermediate vector)

  /************************************************
   *                  SORT PHASE                   *
   ************************************************/


  if (!context->thread_intermediate_vecs[thread_ind].empty ())
  {
    std::sort (this->thread_intermediate_vecs[].begin (),
               this->thread_intermediate_vecs.end (), Pair2lessthan);

    // List all unique keys (will be used for shuffle)
    std::transform (intermedVecs[i].begin (), intermedVecs[i].end (), back_inserter (this->uniqueK2Vecs[i]), [] (IntermediatePair &pair)
    { return pair.first; });
    IntermediateUniqueKeysVec::iterator it;
    it = std::unique (this->uniqueK2Vecs[i].begin (), this->uniqueK2Vecs[i].end (), K2equals);
    this->uniqueK2Vecs[i].resize ((unsigned long) std::distance (this->uniqueK2Vecs[i].begin (), it));
  }

  /************************************************
   *                  BARRIER                     *
   ************************************************/

  // Barrier for all threads
  context->barrier.barrier (threadIndex);

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

    // Collect all unique keys from all intermediate unique keys vectors
    IntermediateUniqueKeysVec uniKeys; // Example: uniqueK2Vecs = {[1,2,3], [2,3], [1,3]}
    for (int i = 0; i < context->numOfIntermediatesVecs; i++)
    {
      std::copy (context->uniqueK2Vecs[i].begin (), context->uniqueK2Vecs[i].end (), back_inserter (uniKeys));   // 10 20 30 20 10 0  0  0  0
    }
    // Unify into single vector of ordered unique keys
    std::sort (uniKeys.begin (), uniKeys.end (), K2lessthan);
    IntermediateUniqueKeysVec::iterator it;
    it = std::unique (uniKeys.begin (), uniKeys.end (), K2equals);   // 10 20 30 20 10 ?  ?  ?  ?
    uniKeys.resize ((unsigned long) std::distance (uniKeys.begin (), it)); // 10 20 30 20 10
    context->uniqueK2Size = uniKeys.size ();

    /************************************************
    *                  PRODUCE TASKS                *
    ************************************************/
    // Go over ordered unique keys, foreach pop all pairs with this key from all vectors and launch reducer
    while (!uniKeys.empty ())
    {
      // Get current key and extract all its pairs from all vectors
      K2 *currKey = uniKeys.back ();
      uniKeys.pop_back ();
      auto keySpecificVec = IntermediateVec (); // TODO: Free at the end of reducer's procedure
      // Go over all intermediate vectors
      for (int j = 0; j < context->numOfIntermediatesVecs; j++)
      {
        // Extract all pairs with current key (if has any)
        while ((!context->intermedVecs[j].empty ()) &&
               K2equals (context->intermedVecs[j].back ().first, currKey))
        {
          keySpecificVec.push_back (context->intermedVecs[j].back ());
          context->intermedVecs[j].pop_back ();
        }
      } // All pairs with current key were processed into keySpecificVec - ready to reduce!

      if (sem_wait (&context->taskQueueSem) != ErrorCode::SUCCESS)
      {
        fprintf (stderr, "Error: SHUFFLER Mutex (queue mutex) lock failure in waiting thread.\n");
        exit (1);
      }

      context->readyQueue.push_back (keySpecificVec);

      if (sem_post (&context->taskQueueSem) != ErrorCode::SUCCESS)
      {
        fprintf (stderr, "Error: SHUFFLER Mutex (queue mutex) UNlock failure in waiting thread.\n");
        exit (1);
      }

      if (sem_post (&context->taskCountSem) != ErrorCode::SUCCESS)
      {
        fprintf (stderr, "Error: SHUFFLER Failed to post semaphore in shuffle stage.\n");
        exit (1);
      }
    }
    context->shuffleState = ShuffleState::DONE_SHUFFLING;

    //special magic loop to release all remaining threads stuck on sem for task counter:
    for (int i = 0; i < context->numOfIntermediatesVecs; i++)
    {

      if (sem_post (&context->taskCountSem) != ErrorCode::SUCCESS)
      {
        fprintf (stderr, "Error: SHUFFLER Failed to post semaphore in release loop\n");
        exit (1);
      }
      if (sem_post (&context->taskQueueSem) != ErrorCode::SUCCESS)
      {
        fprintf (stderr, "Error: SHUFFLER Failed to post semaphore in release loop\n");
        exit (1);
      }

    }
  }

  /************************************************
   *                  REDUCE                      *
   ************************************************/

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