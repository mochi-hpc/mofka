/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_DATA_IMPL_H
#define MOFKA_DATA_IMPL_H

#include "mofka/Data.hpp"

namespace mofka {

class DataImpl {

    public:

    DataImpl(std::vector<Data::Segment> segments)
    : m_segments(std::move(segments)) {}

    DataImpl(const void* ptr, size_t size)
    : m_segments{{ptr, size}} {}

    DataImpl() = default;

    std::vector<Data::Segment> m_segments;
};

}

#endif
