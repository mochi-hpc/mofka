/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "Result.hpp"
#include "ProducerBatch.hpp"
#include "ActiveProducerBatchQueue.hpp"

#include <mofka/Promise.hpp>
#include <mofka/MofkaProducer.hpp>

#include <diaspora/Producer.hpp>
#include <diaspora/Exception.hpp>
#include <diaspora/TopicHandle.hpp>
#include <diaspora/Future.hpp>

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>
#include <limits>

namespace mofka {

MofkaProducer::MofkaProducer(
        tl::engine engine,
        std::string_view name,
        diaspora::BatchSize batch_size,
        diaspora::MaxNumBatches max_batch,
        diaspora::Ordering ordering,
        std::shared_ptr<MofkaThreadPool> thread_pool,
        std::shared_ptr<MofkaTopicHandle> topic)
: m_engine{std::move(engine)}
, m_name{name}
, m_batch_size{batch_size}
, m_max_batch{max_batch}
, m_ordering{ordering}
, m_thread_pool{std::move(thread_pool)}
, m_topic(std::move(topic))
, m_producer_send_batch(m_engine.define("mofka_producer_send_batch"))
{
    m_batch_queues.resize(m_topic->partitions().size());
}

MofkaProducer::~MofkaProducer() {
    flush();
}

std::shared_ptr<diaspora::TopicHandleInterface> MofkaProducer::topic() const {
    return m_topic;
}

diaspora::Future<std::optional<diaspora::EventID>> MofkaProducer::push(
        diaspora::Metadata metadata,
        diaspora::DataView data,
        std::optional<size_t> partition) {
    /* Step 1: create a future/promise pair for this operation */
    diaspora::Future<std::optional<diaspora::EventID>> future;
    Promise<std::optional<diaspora::EventID>> promise;
    // if the batch size is not adaptive, wait() calls on futures should trigger a flush
    auto on_wait = [this](int timeout_ms) mutable {
        flush().wait(timeout_ms);
    };
    std::tie(future, promise) = m_batch_size != diaspora::BatchSize::Adaptive() ?
        Promise<std::optional<diaspora::EventID>>::CreateFutureAndPromise(std::move(on_wait))
        : Promise<std::optional<diaspora::EventID>>::CreateFutureAndPromise();
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
                auto& partition = m_topic->m_partitions.at(partition_index);
                return std::make_shared<ProducerBatch>(
                            m_name, m_engine,
                            m_topic->m_serializer,
                            partition->m_ph,
                            m_producer_send_batch);
            };
            queue = std::make_shared<ActiveProducerBatchQueue>(
                    std::move(create_new_batch),
                    m_thread_pool,
                    m_batch_size,
                    m_max_batch);
            m_batch_queues[partition_index] = queue;
        }
    }
    queue->push(std::move(metadata), std::move(data), promise);
    return future;
}

diaspora::Future<std::optional<diaspora::Flushed>> MofkaProducer::flush() {
    std::lock_guard<thallium::mutex> guard{m_batch_queues_mtx};
    std::shared_ptr<
        std::vector<
            diaspora::Future<
                std::optional<diaspora::Flushed>>>> futures
        = std::make_shared<decltype(futures)::element_type>();
    futures->reserve(m_batch_queues.size());
    for(auto& p : m_batch_queues) {
        if(p) futures->push_back(p->flush());
    }
    return {
        [futures](int timeout_ms) -> std::optional<diaspora::Flushed> {
            if(futures->empty()) return diaspora::Flushed{};
            if(futures->empty() || timeout_ms <= 0) {
                for(auto& f : *futures) f.wait(timeout_ms);
                futures->clear();
                return diaspora::Flushed{};
            } else {
                auto now = std::chrono::steady_clock::now();
                auto deadline = now + std::chrono::milliseconds{timeout_ms};
                auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();
                while(remaining > 0 && !futures->empty()) {
                    if(futures->back().wait(remaining).has_value()) {
                        futures->pop_back();
                    }
                    now = std::chrono::steady_clock::now();
                    remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();
                }
                if(futures->empty()) return diaspora::Flushed{};
                else return std::nullopt;
            }
        },
        [futures]() -> bool {
            return futures->empty();
        }
    };
}

}
