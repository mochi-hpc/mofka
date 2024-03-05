/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Data.hpp"
#include "mofka/Exception.hpp"

#include "DataImpl.hpp"
#include "PimplUtil.hpp"

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS_NO_CTOR(Data);

Data::Data()
: self(std::make_shared<DataImpl>()) {}

Data::Data(void* ptr, size_t size)
: self(std::make_shared<DataImpl>(ptr, size)) {}

Data::Data(std::vector<Segment> segments)
: self(std::make_shared<DataImpl>(std::move(segments))) {}

const std::vector<Data::Segment>& Data::segments() const {
    return self->m_segments;
}

size_t Data::size() const {
    return self->m_size;
}

}
