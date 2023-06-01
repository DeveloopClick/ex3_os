#include "JobContext.h"

JobContext::JobContext (const MapReduceClient &client,
                        const InputVec &inputVec, OutputVec &outputVec,
                        int multiThreadLevel) :  client(client),
                                                 inputVec(inputVec),
                                                 outputVec(outputVec)
{
//    :  map_atomic_counter (new std::atomic<unsigned long> (0)),
// TODO: check initialization of atomic stuff
// all statically allocated. is ok?

  map_atomic_counter = new std::atomic<unsigned long>();
  shuffle_atomic_counter = new std::atomic<unsigned long>();
  reduce_atomic_counter = new std::atomic<unsigned long>();
  closed_atomic_counter = new std::atomic<unsigned long>();


//  std::atomic_flag flag_waited = ATOMIC_FLAG_INIT;
//  std::atomic_flag flag_map = ATOMIC_FLAG_INIT;
//  std::atomic_flag flag_shuffle = ATOMIC_FLAG_INIT;
//  std::atomic_flag flag_reduce = ATOMIC_FLAG_INIT;

  num_of_threads = multiThreadLevel;
  num_of_intermediate_pairs = 0;

  state = JobState{UNDEFINED_STAGE, 0};
  emit3Mutex = PTHREAD_MUTEX_INITIALIZER;
  emit2Mutex = PTHREAD_MUTEX_INITIALIZER;
  jobStateMutex = PTHREAD_MUTEX_INITIALIZER;
  barrier = new Barrier(multiThreadLevel);
  //barrier2 = new Barrier(multiThreadLevel);

  for (int i = 0; i < num_of_threads; i++)
  {
    this->thread_intermediate_vecs.emplace_back (IntermediateVec ());

  }
}

JobContext::~JobContext ()
{
  delete barrier;
  delete map_atomic_counter;
  delete reduce_atomic_counter;
  delete shuffle_atomic_counter;
  delete closed_atomic_counter;
  if (pthread_mutex_destroy (&emit3Mutex) != 0)
  {
    fprintf (stderr, "error on pthread_mutex_destroy 4");
    exit (1);
  }
  if (pthread_mutex_destroy (&emit2Mutex) != 0)
  {
    fprintf (stderr, "error on pthread_mutex_destroy 4");
    exit (1);
  }
  if (pthread_mutex_destroy (&jobStateMutex) != 0)
  {
    fprintf (stderr, "error on pthread_mutex_destroy 6");
    exit (1);
  }
  //delete barrier2;
//  delete flag_waited;
//  delete flag_map;
//  delete flag_shuffle;
//  delete flag_reduce;
}

