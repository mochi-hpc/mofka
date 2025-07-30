/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PRODUCER_IMPL_H
#define MOFKA_PRODUCER_IMPL_H

#include "MofkaTopicHandle.hpp"
#include "MofkaPartitionInfo.hpp"
#include "MofkaProducerBatch.hpp"
#include "BatchProducer.hpp"
#include "UUID.hpp"

#include <diaspora/TopicHandle.hpp>
#include <diaspora/Producer.hpp>
#include <diaspora/Ordering.hpp>

#include <thallium.hpp>
#include <string_view>
#include <queue>

namespace mofka {

namespace tl = thallium;

class MofkaTopicHandle;

class MofkaProducer : public BatchProducer {

    public:

    tl::engine                        m_engine;
    std::shared_ptr<MofkaTopicHandle> m_mofka_topic;
    tl::remote_procedure              m_producer_send_batch;

    MofkaProducer(tl::engine engine,
                  std::string_view name,
                  diaspora::BatchSize batch_size,
                  diaspora::MaxNumBatches max_batch,
                  diaspora::Ordering ordering,
                  std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
                  std::shared_ptr<MofkaTopicHandle> topic);

    std::shared_ptr<ProducerBatchInterface> newBatchForPartition(size_t index) const override;

    ~MofkaProducer() {
        flush();
    }
};

}

#endif
