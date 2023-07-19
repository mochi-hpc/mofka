/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_DATA_SELECTOR_HPP
#define MOFKA_DATA_SELECTOR_HPP

#include <mofka/Metadata.hpp>
#include <mofka/Data.hpp>
#include <functional>
#include <exception>
#include <stdexcept>

namespace mofka {

/**
 * @brief The DataSelector is a function passed to a Consumer
 * and whose role is to tell it if the Data piece associated with
 * an event should be pulled from the Mofka server.
 */
using DataSelector = std::function<bool(const Metadata& metadata)>;

}

#endif
