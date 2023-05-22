#include <pthread.h>
#include "MapReduceFramework.h"
#include "JobContext.h"


//list<Thread *> *thread_list;

//int pthread_create
//(
// pthread_t *thread,
// const pthread_attr_t *attr=NULL,
// void *(*start_routine) (void*),
// void *arg
// );

pthread_t *thread_list;

struct JobContext
{
    std::atomic <int> map_atomic_counter;
    std::atomic <int> shuffle_atomic_counter;
    std::atomic <int> reduce_atomic_counter;
    const InputVec &inputVec;
    const MapReduceClient &client;
    OutputVec &outputVec;
    std::vector <IntermediateVec> our_queue;
    IntermediateVec *thread_intermediate_vecs;
    int num_of_vecs;
    JobState state;
};

struct ThreadContext
{
    JobContext *context;
    int thread_ind;
};

JobHandle startMapReduceJob (const MapReduceClient &client,
                             const InputVec &inputVec, OutputVec &outputVec,
                             int multiThreadLevel)
{

  if (multiThreadLevel < 1)
  {
    fprintf (stderr, "Error: multithread level %d is illegal.\n", multiThreadLevel);
    exit (-1);
  }

  JobContext *context;
  context->map_atomic_counter.store(0);
  context->inputVec (&inputVec);
  context->client (&client);
  context->outputVec (&outputVec);
  context->num_of_vecs (multiThreadLevel);
  context->state ({UNDEFINED_STAGE, 0});
  thread_list = new pthread_t[multiThreadLevel];

  ThreadContext context_vec[multiThreadLevel];
  for (int i = 0; i < multiThreadLevel; i++)
  {
    context_vec[i] = ThreadContext{context, i};
  }

  for (int i = 1; i < multiThreadLevel; i++)
  {
    if (0 != pthread_create (&thread_list[i], nullptr, run_job,
                             static_cast<void *>(&context_vec[i])))
    {
      fprintf (stderr, "Error: Failure to spawn new thread in run.\n");
      exit (-1);
    }
  }

  return context;
}

void getJobState (JobHandle job, JobState *state)
{
  state = static_cast<JobContext *>(job)->state;
}

void closeJobHandle (JobHandle job)
{

}

void waitForJob (JobHandle job)
{
  for (int i = 1; i < multiThreadLevel; i++)
  {
    if (0 != pthread_join (thread_list[i], NULL))
    {
      fprintf (stderr, "Error: Failure to join threads in run.\n");
      exit (-1);
    }
  }
}

void *run_thread (void *thread_context)
{

  /************************************************
   *                  MAP PHASE                   *
   ************************************************/
  auto thread_context = static_cast<ThreadContext *> (thread_context);
  int thread_ind = thread_context->thread_ind;
  JobContext *context = thread_context->context;

  context->state.stage = MAP_STAGE;

  unsigned long old_value = 0;

  while ((old_value = context->map_atomic_counter++) < context->inputVec
  .length())
  {
    context->client.map (context->inputVec[old_value].first,
                         context->inputVec[old_value].second,
                         thread_context);

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
