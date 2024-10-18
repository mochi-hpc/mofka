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

ThreadCount ThreadPool::threadCount() const{
    return mofka::ThreadCount{self->managed_xstreams_size()};
}

size_t ThreadPool::size() const {
    return self->size();
}

void ThreadPool::pushWork(std::function<void()> func, uint64_t priority) const {
    self->pushWork(std::move(func), priority);
}

}
