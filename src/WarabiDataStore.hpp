/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_WARABI_DATA_STORE_HPP
#define MOFKA_WARABI_DATA_STORE_HPP

#include "JsonUtil.hpp"
#include <warabi/Client.hpp>
#include <warabi/TargetHandle.hpp>
#include <mofka/Result.hpp>
#include <mofka/Metadata.hpp>
#include <mofka/DataDescriptor.hpp>
#include <mofka/BulkRef.hpp>
#include <spdlog/spdlog.h>
#include <cstddef>
#include <string_view>
#include <unordered_map>

namespace mofka {

class WarabiDataStore {

    struct WarabiDataDescriptor {
        size_t           offset;
        warabi::RegionID region_id;
    };

    thallium::engine     m_engine;
    warabi::TargetHandle m_target;

    public:

    Result<std::vector<DataDescriptor>> store(
            size_t count,
            const BulkRef& remoteBulk) {

        /* prepare the result array and its content by resizing the location
         * field to be able to hold a WarabiDataDescriptor. */
        Result<std::vector<DataDescriptor>> result;
        result.value().resize(count);
        for(auto& descriptor : result.value()) {
            auto& location = descriptor.location();
            location.resize(sizeof(WarabiDataDescriptor));
        }

        /* lookup the sender. */
        const auto source = m_engine.lookup(remoteBulk.address);

        /* compute the offset at which the data start in the bulk handle
         * (the first count*sizeof(size_t) bytes hold the data sizes). */
        const auto dataOffset = count*sizeof(size_t);

        /* create a local buffer to receive the sizes (these sizes are
         * needed later to make the DataDescriptors). */
        std::vector<size_t> sizes(count);
        auto sizesBulk = m_engine.expose(
            {{sizes.data(), dataOffset}},
            thallium::bulk_mode::write_only);

        // FIXME: the two following steps could be done in parallel.

        /* transfer size of each region */
        sizesBulk << remoteBulk.handle.on(source)(remoteBulk.offset, dataOffset);

        /* forward data as region into Warabi */
        warabi::RegionID region_id;
        m_target.createAndWrite(
            &region_id, remoteBulk.handle, remoteBulk.address,
            remoteBulk.offset + dataOffset, remoteBulk.size - dataOffset, true);

        /* update the result vector */
        WarabiDataDescriptor wdescriptor{0, region_id};
        for(size_t j = 0; j < count; ++j) {
            result.value()[j] = DataDescriptor::From(
                std::string_view{
                    reinterpret_cast<char*>(&wdescriptor),
                    sizeof(wdescriptor)
                },
                sizes[j]);
            wdescriptor.offset += sizes[j];
        }

        return result;
    }

    std::vector<Result<void>> load(
        const std::vector<DataDescriptor>& descriptors,
        const BulkRef& remoteBulk) {

        std::vector<Result<void>> result;
        result.resize(descriptors.size());

        auto getWarabiDataDescriptor = [&descriptors](size_t i) {
            return reinterpret_cast<const WarabiDataDescriptor*>(
                        descriptors[i].location().data()
            );
        };

        // issue all the reads in parallel
        // FIXME: it would be better to be able to group by region but this would mean
        // having an API for fragmented region to fragmented bulk in Warabi
        // FIXME: the code bellow doesn't work if the DataDescriptor is not just a location
        std::vector<warabi::AsyncRequest> requests(descriptors.size());
        size_t currentOffset = remoteBulk.offset;
        for(size_t i = 0; i < descriptors.size(); ++i) {
            const auto descriptor     = getWarabiDataDescriptor(i);
            const auto region         = descriptor->region_id;
            const auto offsetInRegion = descriptor->offset;
            const auto size           = descriptors[i].size();
            m_target.read(region, {{offsetInRegion, size}},
                          remoteBulk.handle,
                          remoteBulk.address,
                          currentOffset,
                          &requests[i]);
            currentOffset += size;
        }

        // wait for all the requests
        for(size_t i = 0; i < requests.size(); ++i) {
            try {
                requests[i].wait();
            } catch(const warabi::Exception& ex) {
                result[i].success() = false;
                result[i].error() = ex.what();
            }
        }

        return result;
    }

    WarabiDataStore(
            thallium::engine engine,
            warabi::TargetHandle target)
    : m_engine(std::move(engine))
    , m_target(std::move(target)) {}

    static std::unique_ptr<WarabiDataStore> create(thallium::engine engine, warabi::TargetHandle target) {
        return std::make_unique<WarabiDataStore>(std::move(engine), std::move(target));
    }

};

} // namespace mofka

#endif
