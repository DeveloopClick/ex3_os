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

list <pthread_t*> *thread_list;
int thread_num;

struct JobContext
    {
    std::atomic<uint64_t>* atomic_counter;
    const InputVec &inputVec;
    const MapReduceClient &client;
    OutputVec &outputVec;
    std::vector<IntermediateVec> our_queue;
    IntermediateVec *thread_intermediate_vecs;
    int thread_index;
    int num_of_vecs;
    stage_t state;
};


JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel)
{
  std::atomic<uint64_t> atomic_counter(0);
  JobContext *context;
  context->atomic_counter(&atomic_counter)
  context->inputVec(&inputVec)
  context->client(&client)
  context->outputVec(&outputVec)
  if (multiThreadLevel < 1)
  {
    fprintf (stderr, "Error: multithread level %d is illegal.\n", multiThreadLevel);
    exit (-1);
  }
  thread_num = multiThreadLevel

  for (auto const &thread: *thread_list)
  {
    if (0 != pthread_create (&thread, nullptr, run_thread,
                             static_cast<void *>(&context)))
    {
      fprintf (stderr, "Error: Failure to spawn new thread in run.\n");
      exit (-1);
    }
  }
}

void waitForJob(JobHandle job)
{

}

void getJobState(JobHandle job, JobState* state)
{
  state->stage=static_cast<JobContext*>(job)->state;
}
void closeJobHandle(JobHandle job);





void waitForJob(JobHandle job)
{
  if (0 != pthread_join(threads[t_index], NULL))
  {
    fprintf(stderr, "Error: Failure to join threads in run.\n");
    exit(-1);
  }
}


void getJobState(JobHandle job, JobState* state)
{

}


void * run_thread() {

  /************************************************
   *                  MAP PHASE                   *
   ************************************************/

  unsigned long old_value = 0;
  while(old_value < context->context) {
    context->client.map( context->inputVec[old_value].first,
                         context->inputVec[old_value].second,
                         context);
    old_value = atomic_counter.load() >> 33;
    uint64_t value = atomic_counter.load();
    value += (1ULL << 33);
    atomic_counter.store(value);

  }

  // My intermediate vector assumed to be populated at this point
  // Sorting Stage - No mutually shared objects
  context->prepareForShuffle(threadIndex);

  /************************************************
   *                  BARRIER                     *
   ************************************************/

  // Barrier for all threads
  context->barrier.barrier(threadIndex);

  /************************************************
   *              SHUFFLE \ REDUCE                *
   ************************************************/

  /************************************************
   *                  SHUFFLE                     *
   ************************************************/
  // first to retrive 0 (atomically) is crowned shuffler. Long shall he reign!
  int first = context->shufflerRace++;
  if (first == 0){

    // lock for the rest of the threads
    context->shuffleState = ShuffleState::IN_SHUFFLE;

    // Collect all unique keys from all intermediate unique keys vectors
    IntermediateUniqueKeysVec uniKeys; // Example: uniqueK2Vecs = {[1,2,3], [2,3], [1,3]}
    for (int i = 0; i < context->numOfIntermediatesVecs; i++) {
      std::copy(context->uniqueK2Vecs[i].begin(), context->uniqueK2Vecs[i].end(), back_inserter(uniKeys));   // 10 20 30 20 10 0  0  0  0
    }
    // Unify into single vector of ordered unique keys
    std::sort(uniKeys.begin(), uniKeys.end(), K2lessthan);
    IntermediateUniqueKeysVec::iterator it;
    it = std::unique(uniKeys.begin(), uniKeys.end(), K2equals);   // 10 20 30 20 10 ?  ?  ?  ?
    uniKeys.resize((unsigned long)std::distance(uniKeys.begin(), it) ); // 10 20 30 20 10
    context->uniqueK2Size = uniKeys.size();

    /************************************************
    *                  PRODUCE TASKS                *
    ************************************************/
    // Go over ordered unique keys, foreach pop all pairs with this key from all vectors and launch reducer
    while(!uniKeys.empty()){
      // Get current key and extract all its pairs from all vectors
      K2* currKey = uniKeys.back();
      uniKeys.pop_back();
      auto keySpecificVec = IntermediateVec(); // TODO: Free at the end of reducer's procedure
      // Go over all intermediate vectors
      for (int j = 0; j < context->numOfIntermediatesVecs; j++) {
        // Extract all pairs with current key (if has any)
        while ((!context->intermedVecs[j].empty()) &&
               K2equals(context->intermedVecs[j].back().first, currKey)){
          keySpecificVec.push_back(context->intermedVecs[j].back());
          context->intermedVecs[j].pop_back();
        }
      } // All pairs with current key were processed into keySpecificVec - ready to reduce!

      if (sem_wait(&context->taskQueueSem) != ErrorCode::SUCCESS)
      {
        fprintf(stderr, "Error: SHUFFLER Mutex (queue mutex) lock failure in waiting thread.\n");
        exit(1);
      }

      context->readyQueue.push_back(keySpecificVec);

      if (sem_post(&context->taskQueueSem) != ErrorCode::SUCCESS)
      {
        fprintf(stderr, "Error: SHUFFLER Mutex (queue mutex) UNlock failure in waiting thread.\n");
        exit(1);
      }

      if (sem_post(&context->taskCountSem) != ErrorCode::SUCCESS)
      {
        fprintf(stderr, "Error: SHUFFLER Failed to post semaphore in shuffle stage.\n");
        exit(1);
      }
    }
    context->shuffleState = ShuffleState::DONE_SHUFFLING;

    //special magic loop to release all remaining threads stuck on sem for task counter:
    for (int i=0; i<context->numOfIntermediatesVecs; i++){

      if (sem_post(&context->taskCountSem) != ErrorCode::SUCCESS)
      {
        fprintf(stderr, "Error: SHUFFLER Failed to post semaphore in release loop\n");
        exit(1);
      }
      if (sem_post(&context->taskQueueSem) != ErrorCode::SUCCESS)
      {
        fprintf(stderr, "Error: SHUFFLER Failed to post semaphore in release loop\n");
        exit(1);
      }

    }
  }

  /************************************************
   *                  REDUCE                      *
   ************************************************/

  // All threads continue here. ShuffleLocked represents the shuffler is still working
  unsigned long task_num = 0;
  while(true){
    if (context->reduceTaskCounter >= context->uniqueK2Size){
      break;
    }

    // Wait for the shuffler to populate queue. Signal comes through semaphore
    if (sem_wait(&context->taskCountSem) != ErrorCode::SUCCESS)
    {
      fprintf(stderr, "Error: REDUCER Semaphore failure in waiting thread.\n");
      exit(1);
    }

    // Lock the mutex to access mutual queue
    if (sem_wait(&context->taskQueueSem) != ErrorCode::SUCCESS)
    {
      fprintf(stderr, "Error: REDUCER Mutex lock failure in waiting thread.\n");
      exit(1);
    }

    // Leave if all tasks were performed
    task_num = context->reduceTaskCounter++;

    IntermediateVec job;
    // retrieve next job if exists
    if (task_num < context->uniqueK2Size){
      job = context->readyQueue[task_num];

    }

    if (sem_post(&context->taskQueueSem) != ErrorCode::SUCCESS)
    {
      fprintf(stderr, "Error: REDUCER Mutex unlock failure in waiting thread.\n");
      exit(1);
    }

    if (task_num < context->uniqueK2Size){
      context->client.reduce(&job, contextWrapper);

    }
  }

  return (void *)ErrorCode::SUCCESS;
}
