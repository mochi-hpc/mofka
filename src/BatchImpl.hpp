/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_BATCH_IMPL_H
#define MOFKA_BATCH_IMPL_H

#include <thallium.hpp>
#include <mutex>
#include "mofka/EventID.hpp"
#include "mofka/Metadata.hpp"
#include "mofka/Archive.hpp"
#include "mofka/Serializer.hpp"
#include "mofka/Data.hpp"
#include "mofka/Future.hpp"
#include "DataImpl.hpp"
#include <vector>
#include <cstdint>

namespace mofka {

class BatchImpl {

    struct BatchOutputArchive : public Archive {

        void read(void* buffer, std::size_t size) override {
            /* this function is not supposed to be called */
            (void)buffer;
            (void)size;
        }

        void write(const void* data, size_t size) override {
            auto new_size = m_buffer.size() + size;
            if(m_buffer.capacity() < m_buffer.size() + size) {
                m_buffer.reserve(2*new_size);
            }
            auto offset = m_buffer.size();
            m_buffer.resize(new_size);
            std::memcpy(m_buffer.data() + offset, data, size);
            m_size += size;
        }

        BatchOutputArchive(size_t& s, std::vector<char>& buf)
        : m_size(s)
        , m_buffer(buf) {}

        size_t&            m_size;
        std::vector<char>& m_buffer;
    };

    mutable thallium::mutex    m_mtx; /* mutex protecting access to the batch */

    std::vector<size_t>        m_meta_sizes;      /* size of each serialized metadata object */
    std::vector<char>          m_meta_buffer;     /* packed serialized metadata objects */
    std::vector<size_t>        m_data_offsets;    /* offset at which the data for each metata starts */
    std::vector<size_t>        m_data_sizes;      /* size of the data associated with each metadata */
    std::vector<Data::Segment> m_data_segments;   /* list of data segments */
    size_t                     m_total_data_size; /* sum of sizes in the m_data_segments */

    using FutureEventID = thallium::eventual<EventID>;

    std::vector<std::weak_ptr<FutureEventID>> m_futures; /* futures associated with each event */

    public:

    Future<EventID> push(const Metadata& metadata,
              const Serializer& serializer,
              const Data& data) {
        size_t meta_size = 0;
        BatchOutputArchive archive(meta_size, m_meta_buffer);
        serializer.serialize(archive, metadata);
        m_meta_sizes.push_back(meta_size);
        m_data_offsets.push_back(m_total_data_size);
        size_t data_size = 0;
        for(const auto& seg : data.self->m_segments) {
            m_data_segments.push_back(seg);
            data_size += seg.size;
        }
        m_data_sizes.push_back(data_size);
        m_total_data_size += data_size;

        auto future_impl = std::make_shared<FutureEventID>();
        m_futures.push_back(future_impl);
        auto wait_fn = [future_impl]() mutable -> EventID {
            return future_impl->wait();
        };
        auto test_fn = [future_impl]() mutable -> bool {
            return future_impl->test();
        };
        return {std::move(wait_fn), std::move(test_fn)};
    }

    thallium::bulk expose(thallium::engine engine) {
        if(count() == 0) return thallium::bulk{};
        std::vector<std::pair<void *, size_t>> segments;
        segments.reserve(4 + m_data_segments.size());
        segments.emplace_back(m_meta_sizes.data(), m_meta_sizes.size()*sizeof(m_meta_sizes[0]));
        segments.emplace_back(m_meta_buffer.data(), m_meta_buffer.size()*sizeof(m_meta_buffer[0]));
        segments.emplace_back(m_data_offsets.data(), m_data_offsets.size()*sizeof(m_data_offsets[0]));
        segments.emplace_back(m_data_sizes.data(), m_data_sizes.size()*sizeof(m_data_sizes[0]));
        for(const auto& seg : m_data_segments) {
            if(!seg.size) continue;
            segments.emplace_back(const_cast<void*>(seg.ptr), seg.size);
        }
        return engine.expose(segments, thallium::bulk_mode::read_only);
    }

    size_t count() const {
        return m_meta_sizes.size();
    }

    size_t totalSize() const {
        return m_total_data_size;
    }

    auto lock() const {
        return std::unique_lock<decltype(m_mtx)>{m_mtx};
    }
};

}

#endif
