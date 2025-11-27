/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PRODUCER_H
#define MOFKA_PRODUCER_H

#include <mofka/MofkaTopicHandle.hpp>
#include <mofka/MofkaPartitionInfo.hpp>
#include <mofka/MofkaThreadPool.hpp>
#include <mofka/UUID.hpp>

#include <diaspora/TopicHandle.hpp>
#include <diaspora/Producer.hpp>
#include <diaspora/Ordering.hpp>

#include <thallium.hpp>
#include <string_view>
#include <queue>

namespace mofka {

namespace tl = thallium;

class MofkaTopicHandle;
class ActiveProducerBatchQueue;

class MofkaProducer : public diaspora::ProducerInterface {

    public:

    tl::engine                        m_engine;
    std::string                       m_name;
    diaspora::BatchSize               m_batch_size;
    diaspora::MaxNumBatches           m_max_batch;
    diaspora::Ordering                m_ordering;
    std::shared_ptr<MofkaThreadPool>  m_thread_pool;
    std::shared_ptr<MofkaTopicHandle> m_topic;
    tl::remote_procedure              m_producer_send_batch;

    std::vector<std::shared_ptr<ActiveProducerBatchQueue>> m_batch_queues;
    thallium::mutex                                        m_batch_queues_mtx;
    thallium::condition_variable                           m_batch_queues_cv;
    std::atomic<size_t>                                    m_num_pushed_events = 0;

    MofkaProducer(tl::engine engine,
                  std::string_view name,
                  diaspora::BatchSize batch_size,
                  diaspora::MaxNumBatches max_batch,
                  diaspora::Ordering ordering,
                  std::shared_ptr<MofkaThreadPool> thread_pool,
                  std::shared_ptr<MofkaTopicHandle> topic);

    ~MofkaProducer();

    const std::string& name() const override {
        return m_name;
    }

    std::shared_ptr<diaspora::TopicHandleInterface> topic() const override;

    diaspora::BatchSize batchSize() const override {
        return m_batch_size;
    }

    diaspora::MaxNumBatches maxNumBatches() const override {
        return m_max_batch;
    }

    diaspora::Ordering ordering() const override {
        return m_ordering;
    }

    std::shared_ptr<diaspora::ThreadPoolInterface> threadPool() const override {
        return m_thread_pool;
    }

    diaspora::Future<std::optional<diaspora::EventID>> push(
            diaspora::Metadata metadata,
            diaspora::DataView data,
            std::optional<size_t> partition) override;

    diaspora::Future<std::optional<diaspora::Flushed>> flush() override;

};

}

#endif
