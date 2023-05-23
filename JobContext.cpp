#include "JobContext.h"

JobContext::JobContext (const MapReduceClient &client,
                        const InputVec &inputVec, OutputVec &outputVec,
                        int multiThreadLevel)
    : client (client), inputVec (inputVec),
      outputVec (outputVec), num_of_intermediate_vecs (multiThreadLevel),
      map_atomic_counter (0), shuffle_atomic_counter (0),
      reduce_atomic_counter (0), state (JobState{UNDEFINED_STAGE, 0}), inVecMutex (PTHREAD_MUTEX_INITIALIZER)
      ,shuffleMutex (PTHREAD_MUTEX_INITIALIZER), outVecMutex
      (PTHREAD_MUTEX_INITIALIZER),barrier(multiThreadLevel),first_to_shuffle(0)
{

}

JobContext::~JobContext ()
{
  if (pthread_mutex_destroy (&inVecMutex) != 0)
  {
    fprintf (stderr, "error on pthread_mutex_destroy");
    exit (1);
  }
  if (pthread_mutex_destroy (&shuffleMutex) != 0)
  {
    fprintf (stderr, "error on pthread_mutex_destroy");
    exit (1);
  }
  if (pthread_mutex_destroy (&outVecMutex) != 0)
  {
    fprintf (stderr, "error on pthread_mutex_destroy");
    exit (1);
  }
  delete[]thread_intermediate_vecs;

}

