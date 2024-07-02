/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_EVENT_ID_HPP
#define MOFKA_EVENT_ID_HPP

#include <mofka/ForwardDcl.hpp>

#include <limits>
#include <cstdint>

namespace mofka {

using EventID = std::uint64_t;

constexpr const EventID NoMoreEvents = std::numeric_limits<EventID>::max();

}

#endif
