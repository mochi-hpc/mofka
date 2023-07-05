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

PIMPL_DEFINE_COMMON_FUNCTIONS(Data);

Data::Data(const void* ptr, size_t size)
: self(std::make_shared<DataImpl>(ptr, size)) {}

Data::Data(std::vector<Segment> segments)
: self(std::make_shared<DataImpl>(std::move(segments))) {}

}
