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

DataDescriptor DataDescriptor::Null() {
    return std::make_shared<DataDescriptorImpl>();
}

size_t DataDescriptor::size() const {
    return self->m_size;
}

DataDescriptor DataDescriptor::makeStridedView(
        size_t offset,
        size_t numblocks,
        size_t blocksize,
        size_t gapsize) const {
    if(offset > self->m_size || numblocks == 0 || blocksize == 0)
        return Null();
    size_t sub_size = self->m_size - offset;
    // find the actual number of blocks that fit in self->m_size
    size_t s = blocksize + gapsize;
    size_t max_num_blocks = std::ceil((double)(sub_size)/(double)s);
    numblocks = std::min(numblocks, max_num_blocks);
    // find out if the last block is cut
    size_t num_full_blocks = sub_size/s;
    size_t remaining = (numblocks - num_full_blocks)*s;
    if(remaining > s) {
        num_full_blocks += 1;
        remaining = 0;
    }
    size_t view_size = num_full_blocks*blocksize + remaining;
    // make the new descriptor
    auto newDesc = std::make_shared<DataDescriptorImpl>(*self);
    newDesc->m_views.emplace_back(
        DataDescriptorImpl::Strided{offset, numblocks, blocksize, gapsize});
    newDesc->m_size = view_size;
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
        const std::map<size_t, size_t>& segments) const {
    if(segments.empty()) return Null();
    if(segments.begin()->first > self->m_size) return Null();
    auto newDesc = std::make_shared<DataDescriptorImpl>(*self);
    size_t view_size = 0;
    size_t current_offset = 0;
    DataDescriptorImpl::Unstructured u;
    for(auto& [offset, size] : segments) {
        if(offset < current_offset)
            throw Exception("Invalid unstructured view: overlapping segments");
        if(!u.segments.empty() && u.segments.back().first + u.segments.back().second == offset) {
            size_t s = std::min(size, self->m_size - current_offset);
            u.segments.back().second += s;
            view_size += s;
            current_offset = offset + s;
        } else {
            size_t s = std::min(size, self->m_size - current_offset);
            u.segments.emplace_back(offset, s);
            view_size += s;
            current_offset = offset + s;
        }
        if(current_offset == self->m_size)
            break;
    }
    if(u.segments.size() == 0)
        return Null();
    if(u.segments.size() == 1)
        return makeSubView(u.segments[0].first, u.segments[0].second);
    newDesc->m_size = view_size;
    return newDesc;
}

}
