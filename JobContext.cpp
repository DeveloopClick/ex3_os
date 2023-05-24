#include "JobContext.h"

JobContext::JobContext (const MapReduceClient &client,
                        const InputVec &inputVec, OutputVec &outputVec,
                        int multiThreadLevel)
    : client (client),
      inputVec (inputVec),
      outputVec (outputVec),
      num_of_threads (multiThreadLevel),
      num_of_intermediate_pairs (0),
      flag_waited(0),
      map_atomic_counter (0),
      shuffle_atomic_counter (0),
      reduce_atomic_counter (0),
      closed_atomic_counter (0),
      state (JobState{UNDEFINED_STAGE, 0}),
      inVecMutex (PTHREAD_MUTEX_INITIALIZER),
      shuffleMutex (PTHREAD_MUTEX_INITIALIZER),
      outVecMutex (PTHREAD_MUTEX_INITIALIZER),
      emit2Mutex (PTHREAD_MUTEX_INITIALIZER),
      emit3Mutex (PTHREAD_MUTEX_INITIALIZER),
      jobStateMutex (PTHREAD_MUTEX_INITIALIZER),
      sortMutex (PTHREAD_MUTEX_INITIALIZER),
      cv(PTHREAD_COND_INITIALIZER),
      barrier (multiThreadLevel),
      first_to_map (0),
      first_to_shuffle (0),
      first_to_reduce (0)
{
//  this->thread_intermediate_vecs = new
//      IntermediateVec[num_of_intermediate_vecs];
  for (int i = 0; i < num_of_threads; i++)
  {
    this->thread_intermediate_vecs.emplace_back (IntermediateVec ());

  }
}

JobContext::~JobContext ()
{
  if (pthread_mutex_destroy (&inVecMutex) != 0)
  {
    fprintf (stderr, "error on pthread_mutex_destroy");
    exit (1);
  }

  if (pthread_mutex_destroy (&outVecMutex) != 0)
  {
    fprintf (stderr, "error on pthread_mutex_destroy");
    exit (1);
  }
  if (pthread_mutex_destroy (&sortMutex) != 0)
  {
    fprintf (stderr, "error on pthread_mutex_destroy");
    exit (1);
  }
  if (pthread_mutex_destroy (&emit3Mutex) != 0)
  {
    fprintf (stderr, "error on pthread_mutex_destroy");
    exit (1);
  }
  if (pthread_mutex_destroy (&emit2Mutex) != 0)
  {
    fprintf (stderr, "error on pthread_mutex_destroy");
    exit (1);
  }
  if (pthread_mutex_destroy (&jobStateMutex) != 0)
  {
    fprintf (stderr, "error on pthread_mutex_destroy");
    exit (1);
  }
  if (pthread_mutex_destroy (&shuffleMutex) != 0)
  {
    fprintf (stderr, "error on pthread_mutex_destroy");
    exit (1);
  }

  if (pthread_cond_destroy(&cv) != 0)
  {
    fprintf(stderr, "[[Barrier]] error on pthread_cond_destroy");
    exit(1);
  }
//  delete[] thread_intermediate_vecs;
}

