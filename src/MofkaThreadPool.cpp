/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <diaspora/Exception.hpp>

#include "MofkaThreadPool.hpp"

namespace mofka {

thallium::pool MofkaThreadPool::s_default_pool = thallium::pool{};

}
