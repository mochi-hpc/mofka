/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_DATA_BROKER_HPP
#define MOFKA_DATA_BROKER_HPP

#include <mofka/Metadata.hpp>
#include <mofka/Data.hpp>
#include <mofka/DataDescriptor.hpp>
#include <functional>
#include <exception>
#include <stdexcept>

namespace mofka {

/**
 * @brief DataBroker is the type of a function that takes the
 * Metadata of an event as well as the DataDescriptor of the associated
 * data, and returns a Data object indicating where in memory the data
 * of the event should be placed by the Consumer.
 */
using DataBroker = std::function<Data(const Metadata&, const DataDescriptor&)>;

}

#endif
