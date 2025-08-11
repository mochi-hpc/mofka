/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_WARABI_DATA_STORE_HPP
#define MOFKA_WARABI_DATA_STORE_HPP

#include "JsonUtil.hpp"
#include <mofka/BulkRef.hpp>
#include <mofka/Promise.hpp>
#include "Result.hpp"

#include <diaspora/Metadata.hpp>
#include <diaspora/DataDescriptor.hpp>

#include <warabi/Client.hpp>
#include <warabi/TargetHandle.hpp>

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
    warabi::Client       m_warabi_client;
    warabi::TargetHandle m_target;
    thallium::pool       m_pool;

    public:

    diaspora::Future<std::vector<diaspora::DataDescriptor>> store(
            size_t count,
            const BulkRef& remoteBulk) {

        diaspora::Future<std::vector<diaspora::DataDescriptor>> future;
        Promise<std::vector<diaspora::DataDescriptor>> promise;
        std::tie(future, promise) = Promise<std::vector<diaspora::DataDescriptor>>::CreateFutureAndPromise();

        auto ult = [promise=std::move(promise), count, remoteBulk, this]() mutable {
            /* prepare the result array and its content by resizing the location
             * field to be able to hold a WarabiDataDescriptor. */
            std::vector<diaspora::DataDescriptor> result;
            result.resize(count);
            for(auto& descriptor : result) {
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
            std::vector<size_t> sizes(count, 0);
            auto sizesBulk = m_engine.expose(
                    {{sizes.data(), dataOffset}},
                    thallium::bulk_mode::write_only);

            /* forward data as region into Warabi, if size > 0 */
            warabi::RegionID region_id;
            warabi::AsyncRequest req;
            if(remoteBulk.size - dataOffset > 0) {
                m_target.createAndWrite(
                        &region_id, remoteBulk.handle, remoteBulk.address,
                        remoteBulk.offset + dataOffset, remoteBulk.size - dataOffset, true,
                        &req);
                /* transfer size of each region */
                sizesBulk << remoteBulk.handle.on(source)(remoteBulk.offset, dataOffset);
            } else {
                memset(region_id.data(), 0, region_id.size());
            }

            if(req) req.wait();

            /* update the result vector */
            WarabiDataDescriptor wdescriptor{0, region_id};
            for(size_t j = 0; j < count; ++j) {
                if(sizes[j] > 0) {
                    result[j] = diaspora::DataDescriptor(
                            std::string_view{
                                reinterpret_cast<char*>(&wdescriptor),
                                sizeof(wdescriptor)
                            },
                            sizes[j]);
                    wdescriptor.offset += sizes[j];
                }
            }

            promise.setValue(std::move(result));
        };

        m_pool.make_thread(std::move(ult), thallium::anonymous{});

        return future;
    }

    std::vector<Result<void>> load(
        const std::vector<diaspora::DataDescriptor>& descriptors,
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
            if(descriptors[i].size() == 0) continue;
            const auto descriptor         = getWarabiDataDescriptor(i);
            const auto region             = descriptor->region_id;
            const auto baseOffsetInRegion = descriptor->offset;
            auto flattened = descriptors[i].flatten();
            std::vector<std::pair<size_t, size_t>> regionOffsetSizes(flattened.size());
            for(size_t j = 0; j < flattened.size(); ++j) {
                regionOffsetSizes[j].first  = baseOffsetInRegion + flattened[j].offset;
                regionOffsetSizes[j].second = flattened[j].size;
            }
            m_target.read(region, regionOffsetSizes,
                          remoteBulk.handle,
                          remoteBulk.address,
                          currentOffset,
                          &requests[i]);
            currentOffset += descriptors[i].size();
        }

        // wait for all the requests
        for(size_t i = 0; i < requests.size(); ++i) {
            if(!requests[i]) continue;
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
            warabi::Client warabi_client,
            warabi::TargetHandle target,
            thallium::pool pool)
    : m_engine(std::move(engine))
    , m_warabi_client(std::move(warabi_client))
    , m_target(std::move(target))
    , m_pool(pool) {}

    static std::unique_ptr<WarabiDataStore> create(
            thallium::engine engine,
            thallium::provider_handle warabi_ph,
            thallium::pool pool) {
        auto warabi_client = warabi::Client{engine};
        auto target = warabi_client.makeTargetHandle(warabi_ph, warabi_ph.provider_id());
        return std::make_unique<WarabiDataStore>(
                std::move(engine), std::move(warabi_client), std::move(target), pool);
    }

};

} // namespace mofka

#endif
