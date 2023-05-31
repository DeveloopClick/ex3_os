#include "JobContext.h"

JobContext::JobContext (const MapReduceClient &client,
                        const InputVec &inputVec, OutputVec &outputVec,
                        int multiThreadLevel)
//    :  map_atomic_counter (new std::atomic<unsigned long> (0)),
      : map_atomic_counter(0),
       shuffle_atomic_counter (0),
       reduce_atomic_counter (0),
       closed_atomic_counter (0),

      client (client),
      inputVec (inputVec),
      outputVec (outputVec),

      num_of_threads (multiThreadLevel),
      num_of_intermediate_pairs (0),

      flag_waited(ATOMIC_FLAG_INIT),
      flag_map(ATOMIC_FLAG_INIT),
      flag_shuffle(ATOMIC_FLAG_INIT), // check init
      flag_reduce(ATOMIC_FLAG_INIT), // check init


      state (JobState{UNDEFINED_STAGE, 0}),
      emit3Mutex (PTHREAD_MUTEX_INITIALIZER),
      jobStateMutex (PTHREAD_MUTEX_INITIALIZER),
      barrier (new Barrier(multiThreadLevel))

{

  for (int i = 0; i < num_of_threads; i++)
  {
    this->thread_intermediate_vecs.emplace_back (IntermediateVec ());

  }
}

JobContext::~JobContext ()
{
  if (pthread_mutex_destroy (&emit3Mutex) != 0)
  {
    fprintf (stderr, "error on pthread_mutex_destroy 4");
    exit (1);
  }
  if (pthread_mutex_destroy (&jobStateMutex) != 0)
  {
    fprintf (stderr, "error on pthread_mutex_destroy 6");
    exit (1);
  }
//  delete barrier;
}

