/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/DataDescriptor.hpp"
#include "mofka/Exception.hpp"

#include "DataDescriptorImpl.hpp"
#include "PimplUtil.hpp"

#include <cmath>

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS_NO_CTOR(DataDescriptor);

DataDescriptor::DataDescriptor()
: self(std::make_shared<DataDescriptorImpl>()) {}

DataDescriptor DataDescriptor::Null() {
    return std::make_shared<DataDescriptorImpl>();
}

DataDescriptor DataDescriptor::From(std::string_view location, size_t size) {
    return std::make_shared<DataDescriptorImpl>(location, size);
}

size_t DataDescriptor::size() const {
    return self->m_size;
}

const std::vector<char>& DataDescriptor::location() const {
    return self->m_location;
}

std::vector<char>& DataDescriptor::location() {
    return self->m_location;
}

std::vector<DataDescriptor::Segment> DataDescriptor::flatten() const {
    return self->flatten();
}

DataDescriptor DataDescriptor::makeStridedView(
        size_t offset,
        size_t numblocks,
        size_t blocksize,
        size_t gapsize) const {
    if(offset > self->m_size || numblocks == 0 || blocksize == 0)
        return Null();
    // check that the stride doesn't exceeds the available size
    if(offset + numblocks*(blocksize + gapsize) > self->m_size)
        throw Exception{"Invalid strided view: would go out of bounds"};

    // make the new descriptor
    auto newDesc = std::make_shared<DataDescriptorImpl>(*self);
    newDesc->m_views.emplace_back(
        DataDescriptorImpl::Strided{offset, numblocks, blocksize, gapsize});
    newDesc->m_size = numblocks*blocksize;
    // TODO optimize further the content of the new descriptor
    return newDesc;
}

DataDescriptor DataDescriptor::makeSubView(
        size_t offset,
        size_t size) const {
    if(offset > self->m_size || size == 0 || self->m_size == 0)
        return Null();
    auto newDesc = std::make_shared<DataDescriptorImpl>(*self);
    size = std::min(newDesc->m_size - offset, size);
    newDesc->m_views.emplace_back(DataDescriptorImpl::Sub{offset, size});
    newDesc->m_size = size;
    // TODO optimize further the content of the new descriptor
    return newDesc;
}


DataDescriptor DataDescriptor::makeUnstructuredView(
        const std::vector<std::pair<size_t, size_t>>& segments) const {
    if(segments.empty()) return Null();
    if(segments.begin()->first > self->m_size) return Null();
    auto newDesc = std::make_shared<DataDescriptorImpl>(*self);
    size_t view_size = 0;
    size_t current_offset = 0;
    DataDescriptorImpl::Unstructured u;
    for(auto& [offset, size] : segments) {
        if(offset < current_offset)
            throw Exception("Invalid unstructured view: segments overlapping or out of order");
        if(offset + size > self->m_size)
            throw Exception("Invalid unstructured view: would go out of bounds");
        if(!u.segments.empty() && u.segments.back().offset + u.segments.back().size == offset) {
            u.segments.back().size += size;
            view_size += size;
            current_offset = offset + size;
        } else {
            u.segments.emplace_back(Segment{offset, size});
            view_size += size;
            current_offset = offset + size;
        }
    }
    if(u.segments.size() == 0)
        return Null();
    if(u.segments.size() == 1)
        return makeSubView(u.segments[0].offset, u.segments[0].size);
    newDesc->m_size = view_size;
    newDesc->m_views.push_back(std::move(u));
    return newDesc;
}

void DataDescriptor::load(Archive &ar) {
    self->load(ar);
}

void DataDescriptor::save(Archive& ar) const {
    self->save(ar);
}

}
