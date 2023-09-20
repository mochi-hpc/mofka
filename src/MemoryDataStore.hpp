/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_MEMORY_DATA_STORE_HPP
#define MOFKA_MEMORY_DATA_STORE_HPP

#include <mofka/DataStore.hpp>

namespace mofka {

class MemoryDataStore : public DataStore {

    thallium::engine    m_engine;
    Metadata            m_config;
    std::vector<char>   m_data;
    std::vector<size_t> m_sizes;
    thallium::mutex     m_mutex;

    public:

    MemoryDataStore(thallium::engine engine, Metadata config)
    : m_engine(std::move(engine))
    , m_config(std::move(config)) {}

    MemoryDataStore(MemoryDataStore&&) = default;
    MemoryDataStore& operator=(MemoryDataStore&&) = default;
    virtual ~MemoryDataStore() = default;

    RequestResult<std::vector<DataDescriptor>> store(
            size_t count,
            const BulkRef& sizes,
            const BulkRef& data) override {
        RequestResult<std::vector<DataDescriptor>> result;
        // lookup addresses
        auto size_source = m_engine.lookup(sizes.address);
        auto data_source = sizes.address == data.address ? size_source : m_engine.lookup(data.address);
        // lock the MemoryDataStore
        auto guard = std::unique_lock<thallium::mutex>{m_mutex};
        // resize the m_sizes vector
        auto old_count = m_sizes.size();
        auto new_count = m_sizes.size() + count;
        if(m_sizes.capacity() < new_count) {
            m_sizes.reserve(2*new_count);
        }
        m_sizes.resize(new_count);
        // expose the m_sizes for bulk transfer
        auto localSizesBulk = m_engine.expose(
            {{(void*)(m_sizes.data() + old_count), count*sizeof(size_t)}},
            thallium::bulk_mode::write_only);
        // pull sizes from the sender
        localSizesBulk << sizes.handle.on(size_source)(sizes.offset, sizes.size);
        // resize the m_data vector
        auto old_size = m_data.size();
        auto new_size = m_data.size() + data.size;
        if(m_data.capacity() < new_size) {
            m_data.reserve(2*new_size);
        }
        m_data.resize(new_size);
        // expose the m_data vector for bulk transfer
        auto localDataBulk = m_engine.expose(
            {{(void*)(m_data.data() + old_size), data.size}},
            thallium::bulk_mode::write_only);
        // pull data from the sender
        localDataBulk << data.handle.on(data_source)(data.offset, data.size);
        // create the DataDescriptors
        result.value().reserve(count);
        size_t offset = old_size;
        for(size_t i = 0; i < count; ++i) {
            auto size = m_sizes[old_count + i];
            result.value().push_back(
                DataDescriptor::From(
                    std::string_view{reinterpret_cast<char*>(&offset), sizeof(offset)},
                    size)
            );
            offset += size;
        }
        return result;
    }

    RequestResult<void> load(
        const std::vector<DataDescriptor>& descriptors,
        const BulkRef& dest) override {
        RequestResult<void> result;

        // lock the MemoryDataStore
        auto guard = std::unique_lock<thallium::mutex>{m_mutex};

        // convert descriptors into pointer/size pairs
        std::vector<std::pair<void*, size_t>> segments;
        segments.reserve(descriptors.size());
        for(auto& d : descriptors) {
            if(d.size() == 0) continue;
            size_t offset;
            std::memcpy(&offset, d.location().data(), sizeof(offset));
            segments.push_back({
                static_cast<void*>(m_data.data() + offset),
                d.size()
            });
        }

        // expose the m_data vector for bulk transfer
        auto localDataBulk = m_engine.expose(
            segments,
            thallium::bulk_mode::read_only);

        // do the bulk transfer
        auto sender = m_engine.lookup(dest.address);
        dest.handle.on(sender)(dest.offset, dest.size) << localDataBulk;

        return result;
    }

    RequestResult<bool> destroy() override {
        // lock the MemoryDataStore
        auto guard = std::unique_lock<thallium::mutex>{m_mutex};
        // remove all the data
        m_sizes.clear();
        m_data.clear();
        return RequestResult<bool>{true};
    }

    static std::unique_ptr<DataStore> create(
        const thallium::engine& engine,
        const mofka::Metadata& config) {
        return std::unique_ptr<DataStore>(new MemoryDataStore(engine, config));
    }

};

} // namespace mofka

#endif
