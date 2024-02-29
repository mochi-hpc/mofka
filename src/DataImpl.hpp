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

    DataImpl(std::vector<Data::Segment> segments)
    : m_segments(std::move(segments)) {
        for(auto& s : m_segments) {
            m_size += s.size;
        }
    }

    DataImpl(const void* ptr, size_t size)
    : m_segments{{ptr, size}}
    , m_size(size) {}

    DataImpl() = default;

    ~DataImpl() {
        if(m_ctx_free) m_ctx_free(m_ctx);
    }

    std::vector<Data::Segment> m_segments;
    size_t                     m_size = 0;
    void*                      m_ctx = nullptr;
    void (*m_ctx_free)(void*) = nullptr;
};

}

#endif
