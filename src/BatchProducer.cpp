/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <diaspora/Producer.hpp>
#include <diaspora/Exception.hpp>
#include <diaspora/TopicHandle.hpp>

#include "Promise.hpp"
#include "BatchProducer.hpp"
#include "ActiveProducerBatchQueue.hpp"

namespace mofka {

std::shared_ptr<diaspora::TopicHandleInterface> BatchProducer::topic() const {
    return m_topic;
}

diaspora::Future<diaspora::EventID> BatchProducer::push(
        diaspora::Metadata metadata,
        diaspora::DataView data,
        std::optional<size_t> partition) {
    /* Step 1: create a future/promise pair for this operation */
    diaspora::Future<diaspora::EventID> future;
    Promise<diaspora::EventID> promise;
    // if the batch size is not adaptive, wait() calls on futures should trigger a flush
    auto on_wait = [this]() mutable { flush(); };
    std::tie(future, promise) = m_batch_size != diaspora::BatchSize::Adaptive() ?
        Promise<diaspora::EventID>::CreateFutureAndPromise(std::move(on_wait))
        : Promise<diaspora::EventID>::CreateFutureAndPromise();
    /* Validate the metadata */
    m_topic->validator().validate(metadata, data);
    /* Select the partition for this metadata */
    auto partition_index = m_topic->selector().selectPartitionFor(metadata, partition);
    /* Find/create the ActiveProducerBatchQueue to send to */
    std::shared_ptr<ActiveProducerBatchQueue> queue;
    {
        std::unique_lock<thallium::mutex> guard{m_batch_queues_mtx};
        if(m_batch_queues[partition_index]) {
            queue = m_batch_queues[partition_index];
        } else {
            auto create_new_batch = [this, partition_index]() {
                return newBatchForPartition(partition_index);
            };
            queue = std::make_shared<ActiveProducerBatchQueue>(
                    std::move(create_new_batch),
                    m_thread_pool,
                    batchSize(),
                    maxNumBatches());
            m_batch_queues[partition_index] = queue;
        }
    }
    queue->push(std::move(metadata), std::move(data), promise);
    return future;
}

void BatchProducer::flush() {
    {
        std::lock_guard<thallium::mutex> guard{m_batch_queues_mtx};
        std::vector<diaspora::Future<void>> futures;
        futures.reserve(m_batch_queues.size());
        for(auto& p : m_batch_queues) {
            if(p) futures.push_back(p->flush());
        }
        for(auto& f : futures) {
            f.wait();
        }
    }
}

}
