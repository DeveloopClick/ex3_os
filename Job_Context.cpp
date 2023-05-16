#include "Job_Context.h"
JobContext::~JobContext()
{
  delete thread_intermediate_vecs;
}

