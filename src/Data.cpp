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

Data::Data(Context ctx, FreeCallback free_cb)
: self(std::make_shared<DataImpl>(ctx, std::move(free_cb))) {}

Data::Data(void* ptr, size_t size, Context ctx, FreeCallback free_cb)
: self(std::make_shared<DataImpl>(ptr, size, ctx, std::move(free_cb))) {}

Data::Data(std::vector<Segment> segments, Context ctx, FreeCallback free_cb)
: self(std::make_shared<DataImpl>(std::move(segments), ctx, std::move(free_cb))) {}

const std::vector<Data::Segment>& Data::segments() const {
    return self->m_segments;
}

size_t Data::size() const {
    return self->m_size;
}

Data::Context Data::context() const {
    return self->m_context;
}

}
