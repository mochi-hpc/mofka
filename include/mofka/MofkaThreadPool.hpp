/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_THREAD_POOL_H
#define MOFKA_THREAD_POOL_H

#include <diaspora/ThreadPool.hpp>
#include <thallium.hpp>
#include <diaspora/ArgobotsThreadPool.hpp>
#include <vector>

namespace mofka {

using MofkaThreadPool = diaspora::ArgobotsThreadPool;

}

#endif
