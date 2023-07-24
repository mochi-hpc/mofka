/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_DATA_SELECTOR_HPP
#define MOFKA_DATA_SELECTOR_HPP

#include <mofka/Metadata.hpp>
#include <mofka/DataDescriptor.hpp>
#include <functional>
#include <exception>
#include <stdexcept>

namespace mofka {

/**
 * @brief The DataSelector is a function passed to a Consumer
 * and whose role is to tell it if the Data piece associated with
 * an event should be pulled from the Mofka server, and if so
 * which part of this data, by returning a DataDescriptor instance
 * from the provided DataDescriptor.
 *
 * An easy way to select the whole data is to just return the provided
 * DataDescriptor. An easy way to decline all the data is to just return
 * DataDescriptor::Null().
 */
using DataSelector = std::function<DataDescriptor(const Metadata&, const DataDescriptor&)>;

}

#endif
