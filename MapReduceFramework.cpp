#include <pthread.h>

typedef struct {
    JobState state;

} JobContext;

list<Thread *> *thread_list;

int pthread_create
(
 pthread_t *thread,
 const pthread_attr_t *attr=NULL,
 void *(*start_routine) (void*),
 void *arg
 );
