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
#include "ClientImpl.hpp"
#include "ProducerImpl.hpp"
#include "PimplUtil.hpp"
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
    /* Step 1: validate the metadata */
    self->m_topic->m_validator.validate(metadata, data);
    /* Step 2: select the target */
    auto target = self->m_topic->m_selector.selectTargetFor(metadata);
    /* Step 3: find a batch associated with the target */
    std::shared_ptr<BatchImpl> batch;
    std::scoped_lock<thallium::mutex> guard{self->m_pending_batches_mtx};
    auto it = self->m_pending_batches.find(target);
    if(it == self->m_pending_batches.end()) {
        std::tie(it, std::ignore)
            = self->m_pending_batches.emplace(
                    std::make_pair(target, decltype(it->second){}));
    }
    auto& queue = it->second;
    if(queue.empty()) {
        queue.emplace();
    }
    batch = queue.front();
    /* Step 4: push the metadata and data to the batch */
    auto future = batch->push(metadata, self->m_topic->m_serializer, data);
    /* Step 5: propose the batch to the BatchSender for transfer */
    // TODO
    return future;
}

BatchSize BatchSize::Adaptive() {
    return BatchSize{std::numeric_limits<std::size_t>::max()};
}

}
