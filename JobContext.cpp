#include "JobContext.h"



JobContext::~JobContext()
{
  delete thread_intermediate_vecs;
}

