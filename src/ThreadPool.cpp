/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Exception.hpp"
#include "mofka/ThreadPool.hpp"
#include "ThreadPoolImpl.hpp"
#include "PimplUtil.hpp"

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS_NO_CTOR(ThreadPool);

ThreadPool::ThreadPool(mofka::ThreadCount tc)
: self(std::make_shared<ThreadPoolImpl>(tc)) {}

mofka::ThreadCount ThreadPool::threadCount() const{
    return mofka::ThreadCount{self->managed_xstreams_size()};
}

}
