/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_EVENT_PROCESSOR_HPP
#define MOFKA_EVENT_PROCESSOR_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/Event.hpp>

#include <functional>
#include <exception>
#include <stdexcept>

namespace mofka {

/**
 * @brief Throw this from an EventProcessor function to tell
 * the consumer to stop feeding the EventProcessor and get out
 * of the process function.
 */
struct StopEventProcessor {};

using EventProcessor = std::function<Data(const Event& event)>;

}

#endif
