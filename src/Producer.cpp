/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Producer.hpp"
#include "mofka/RequestResult.hpp"
#include "mofka/Exception.hpp"
#include "mofka/TopicHandle.hpp"
#include "mofka/Future.hpp"

#include "AsyncRequestImpl.hpp"
#include "Promise.hpp"
#include "ClientImpl.hpp"
#include "ProducerImpl.hpp"
#include "PimplUtil.hpp"
#include "ThreadPoolImpl.hpp"
#include <limits>

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS(Producer);

const std::string& Producer::name() const {
    return self->m_name;
}

TopicHandle Producer::topic() const {
    return TopicHandle(self->m_topic);
}

BatchSize Producer::batchSize() const {
    return self->m_batch_size;
}

ThreadPool Producer::threadPool() const {
    return self->m_thread_pool;
}

Future<EventID> Producer::push(Metadata metadata, Data data) const {
    /* Step 1: create a future/promise pair for this operation */
    Future<EventID> future;
    Promise<EventID> promise;
    std::tie(future, promise) = Promise<EventID>::CreateFutureAndPromise();
    /* Step 2: create a ULT that will validate, select the target, and serialize */
    auto ult = [this,
                promise=std::move(promise),
                metadata=std::move(metadata),
                data=std::move(data)]() mutable {
        auto topic = self->m_topic;
        try {
            /* Step 2.1: validate the metadata */
            topic->m_validator.validate(metadata, data);
            /* Step 2.2: select the target for this metadata */
            auto target = topic->m_selector.selectTargetFor(metadata);
            /* Step 2.3: find/create the ActiveBatchQueue to send to */
            std::shared_ptr<ActiveBatchQueue> queue;
            {
                std::lock_guard<thallium::mutex> guard{self->m_batch_queues_mtx};
                auto& queue_ptr = self->m_batch_queues[target];
                if(!queue_ptr) {
                    queue_ptr.reset(new ActiveBatchQueue{threadPool()});
                }
                queue = queue_ptr;
            }
            /* Step 2.4: push the data and metadata to the batch */
            queue->push(metadata, topic->m_serializer, data, promise);
            /* Step 2.5: now the ActiveBatchQueue ULT will automatically
             * pick up the batch and send it when needed */
        } catch(const Exception& ex) {
            promise.setException(ex);
        }
    };
    /* Step 3: submit the ULT */
    self->m_thread_pool.self->pushWork(std::move(ult));
    /* Step 4: return the future */
    return future;
}

BatchSize BatchSize::Adaptive() {
    return BatchSize{std::numeric_limits<std::size_t>::max()};
}

}
