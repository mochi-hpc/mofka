/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_DATA_IMPL_H
#define MOFKA_DATA_IMPL_H

#include "mofka/Data.hpp"
#include <numeric>

namespace mofka {

class DataImpl {

    public:

    DataImpl(std::vector<Data::Segment> segments,
             Data::Context ctx = nullptr,
             Data::FreeCallback&& free_cb = Data::FreeCallback{})
    : m_segments(std::move(segments))
    , m_context{ctx}
    , m_free{std::move(free_cb)} {
        for(auto& s : m_segments) {
            m_size += s.size;
        }
    }

    DataImpl(void* ptr, size_t size,
             Data::Context ctx = nullptr,
             Data::FreeCallback&& free_cb = Data::FreeCallback{})
    : m_segments{{ptr, size}}
    , m_size(size)
    , m_context{ctx}
    , m_free{free_cb} {}

    DataImpl(Data::Context ctx = nullptr,
             Data::FreeCallback&& free_cb = Data::FreeCallback{})
    : m_context{ctx}
    , m_free{std::move(free_cb)} {}

    ~DataImpl() {
        if(m_free) m_free(m_context);
    }

    std::vector<Data::Segment> m_segments;
    size_t                     m_size = 0;
    Data::Context              m_context = nullptr;
    Data::FreeCallback         m_free;
};

}

#endif
