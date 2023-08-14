/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_ORDERING_HPP
#define MOFKA_ORDERING_HPP

#include <mofka/ForwardDcl.hpp>

namespace mofka {

enum class Ordering : bool {
    Strict = true,
    Loose  = false
};

}

#endif
