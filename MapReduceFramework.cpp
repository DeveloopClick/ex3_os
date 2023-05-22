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


void waitForJob(JobHandle job);
void getJobState(JobHandle job, JobState* state)
{
  state->stage=static_cast<JobContext*>(job)->state;
}
void closeJobHandle(JobHandle job);
