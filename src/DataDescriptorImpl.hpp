/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_DATA_DESCRIPTOR_IMPL_H
#define MOFKA_DATA_DESCRIPTOR_IMPL_H

#include "mofka/UUID.hpp"
#include "mofka/Archive.hpp"
#include "mofka/Exception.hpp"
#include "mofka/DataDescriptor.hpp"
#include "VariantUtil.hpp"
#include <iostream>
#include <variant>
#include <vector>
#include <string>
#include <cstdint>
#include <numeric>

namespace mofka {

class DataDescriptorImpl {

    public:

    using Sub = DataDescriptor::Segment;

    struct Strided {
        std::size_t offset;
        std::size_t numblocks;
        std::size_t blocksize;
        std::size_t gapsize;
    };

    struct Unstructured {
        std::vector<Sub> segments;
    };

    enum class ViewType : std::uint8_t {
        SUB, STRIDED, UNSTRUCTURED
    };

    using Selection = std::variant<Sub, Strided, Unstructured>;

    std::vector<Sub> flatten() const {
        std::vector<Sub> flat = {{0, m_base_size}};

        auto parse_sub = [&flat](const Sub& sub) -> decltype(flat) {
            decltype(flat) result;
            size_t cursor = 0;
            size_t remaining_size = sub.size;
            for(auto& segment : flat) {
                if(cursor + segment.size < sub.offset) {
                    // we haven't reached the start of sub yet
                    cursor += segment.size;
                    continue;
                }
                if(cursor >= sub.offset + sub.size) {
                    // we are past the end of the sub
                    break;
                }
                size_t offset, size;
                if(cursor < sub.offset) {
                    offset = segment.offset + sub.offset - cursor;
                } else { // cursor >= sub.offset
                    offset = segment.offset;
                }
                size = segment.size - (offset - segment.offset);
                if(size > remaining_size) {
                    size = remaining_size;
                }
                result.emplace_back(Sub{offset, size});
                remaining_size -= size;
                cursor += segment.size;
            }
            return result;
        };

        auto parse_unstructured = [&flat](const Unstructured& u) -> decltype(flat) {
            decltype(flat) result;
            if(flat.size() != 1) {
                throw Exception{"Stacked \"unstructured\" or \"strided\" descriptors are not yet supported"};
            }
            // flat if of size 1, but it can still start at a particular offset
            result.reserve(u.segments.size());
            for(auto& seg : u.segments) {
                result.push_back(Sub{flat[0].offset + seg.offset, seg.size});
            }
            return result;
        };

        auto parse_strided = [&parse_unstructured](const Strided& strided) -> decltype(flat) {
            Unstructured unstructured;
            unstructured.segments.reserve(strided.numblocks);
            size_t offset = strided.offset;
            for(size_t i = 0; i < strided.numblocks; ++i) {
                unstructured.segments.push_back(Sub{offset, strided.blocksize});
                offset += strided.blocksize + strided.gapsize;
            }
            return parse_unstructured(unstructured);
        };

        auto visitor = Overloaded{
            parse_sub, parse_strided, parse_unstructured
        };
        for(auto& view : m_views)
            flat = std::visit(visitor, view);
        return flat;
    };

    void save(Archive& ar) const {
        auto visitor = Overloaded{
            [&ar](const Sub& sub) {
                auto t = ViewType::SUB;
                ar.write(&t, sizeof(t));
                ar.write(&sub.offset, sizeof(sub.offset));
                ar.write(&sub.size, sizeof(sub.size));
            },
            [&ar](const Strided& strided) {
                auto t = ViewType::STRIDED;
                ar.write(&t, sizeof(t));
                ar.write(&strided.offset, sizeof(strided.offset));
                ar.write(&strided.numblocks, sizeof(strided.numblocks));
                ar.write(&strided.blocksize, sizeof(strided.blocksize));
                ar.write(&strided.gapsize, sizeof(strided.gapsize));
            },
            [&ar](const Unstructured& u) {
                auto t = ViewType::UNSTRUCTURED;
                ar.write(&t, sizeof(t));
                size_t num_segments = u.segments.size();
                ar.write(&num_segments, sizeof(num_segments));
                ar.write(u.segments.data(), num_segments*sizeof(u.segments[0]));
            }
        };
        ar.write(&m_base_size, sizeof(m_base_size));
        ar.write(&m_size, sizeof(m_size));
        size_t location_size = m_location.size();
        ar.write(&location_size, sizeof(location_size));
        ar.write(m_location.data(), location_size);
        size_t num_views = m_views.size();
        ar.write(&num_views, sizeof(num_views));
        for(auto& view : m_views)
            std::visit(visitor, view);
    }

    void load(Archive& ar) {
        ar.read(&m_base_size, sizeof(m_base_size));
        ar.read(&m_size, sizeof(m_size));
        size_t location_size = 0;
        ar.read(&location_size, sizeof(location_size));
        m_location.resize(location_size);
        ar.read(const_cast<char*>(m_location.data()), location_size);
        size_t num_views = 0;
        ar.read(&num_views, sizeof(num_views));
        m_views.resize(0);
        m_views.reserve(num_views);
        for(size_t i=0; i < num_views; ++i) {
            ViewType t;
            ar.read(&t, sizeof(t));
            switch(t) {
            case ViewType::SUB:
                {
                    Sub s{0,0};
                    ar.read(&s.offset, sizeof(s.offset));
                    ar.read(&s.size, sizeof(s.size));
                    m_views.push_back(std::move(s));
                }
                break;
            case ViewType::STRIDED:
                {
                    Strided s;
                    ar.read(&s.offset, sizeof(s.offset));
                    ar.read(&s.numblocks, sizeof(s.numblocks));
                    ar.read(&s.blocksize, sizeof(s.blocksize));
                    ar.read(&s.gapsize, sizeof(s.gapsize));
                    m_views.push_back(std::move(s));
                }
                break;
            case ViewType::UNSTRUCTURED:
                {
                    Unstructured u;
                    size_t num_segments = 0;
                    ar.read(&num_segments, sizeof(num_segments));
                    u.segments.resize(num_segments);
                    ar.read(u.segments.data(), num_segments*sizeof(u.segments[0]));
                    m_views.push_back(std::move(u));
                }
                break;
            }
        }
    }

    DataDescriptorImpl() = default;

    DataDescriptorImpl(std::string_view location, size_t size)
    : m_location(location.data(), location.data() + location.size())
    , m_size(size)
    , m_base_size(size) {}

    DataDescriptorImpl(std::vector<char> location, size_t size)
    : m_location(std::move(location))
    , m_size(size)
    , m_base_size(size) {}

    std::vector<char>      m_location;      /* implementation defined data location */
    std::vector<Selection> m_views;         /* stack of selections on top of the data */
    size_t                 m_size = 0;      /* size of the data after selections applied */
    size_t                 m_base_size = 0; /* size of the underlying contiguous memory */
};

}

#endif
