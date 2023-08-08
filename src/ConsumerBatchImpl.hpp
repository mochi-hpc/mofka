/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_CONSUMER_BATCH_IMPL_H
#define MOFKA_CONSUMER_BATCH_IMPL_H

#include <thallium.hpp>
#include <mutex>
#include <queue>
#include "mofka/BulkRef.hpp"
#include "mofka/RequestResult.hpp"
#include "mofka/EventID.hpp"
#include "mofka/Metadata.hpp"
#include "mofka/Archive.hpp"
#include "mofka/Serializer.hpp"
#include "mofka/Data.hpp"
#include "mofka/Future.hpp"
#include "mofka/Consumer.hpp"
#include "PartitionTargetInfoImpl.hpp"
#include "ThreadPoolImpl.hpp"
#include "ConsumerImpl.hpp"
#include "Promise.hpp"
#include "DataImpl.hpp"
#include <vector>
#include <cstdint>

namespace mofka {

class ConsumerImpl;

class ConsumerBatchImpl {

    friend class ConsumerImpl;

    thallium::engine    m_engine;
    std::vector<size_t> m_meta_sizes;       /* size of each serialized metadata object */
    std::vector<char>   m_meta_buffer;      /* packed serialized metadata objects */
    std::vector<size_t> m_data_desc_sizes;  /* size of the data descriptors associated with each metadata */
    std::vector<char>   m_data_desc_buffer; /* packed data descriptors */

    public:

    ConsumerBatchImpl(thallium::engine engine, size_t count, size_t metadata_size, size_t data_desc_size)
    : m_engine(std::move(engine))
    , m_meta_sizes(count)
    , m_meta_buffer(metadata_size)
    , m_data_desc_sizes(count)
    , m_data_desc_buffer(data_desc_size) {}

    ConsumerBatchImpl(ConsumerBatchImpl&&) = default;
    ConsumerBatchImpl(const ConsumerBatchImpl&) = delete;
    ConsumerBatchImpl& operator=(ConsumerBatchImpl&&) = default;
    ConsumerBatchImpl& operator=(const ConsumerBatchImpl&) = default;
    ~ConsumerBatchImpl() = default;

    void pullFrom(const BulkRef& remote_meta_sizes,
                  const BulkRef& remote_meta_buffer,
                  const BulkRef& remote_desc_sizes,
                  const BulkRef& remote_desc_buffer) {
        std::vector<std::pair<void*, size_t>> segments = {
            {m_meta_sizes.data(), m_meta_sizes.size()*sizeof(m_meta_sizes[0])},
            {m_meta_buffer.data(), m_meta_buffer.size()*sizeof(m_meta_buffer[0])},
            {m_data_desc_sizes.data(), m_data_desc_sizes.size()*sizeof(m_data_desc_sizes[0])},
            {m_data_desc_buffer.data(), m_data_desc_buffer.size()*sizeof(m_data_desc_buffer[0])}
        };
        auto local_bulk = m_engine.expose(segments, thallium::bulk_mode::write_only);
        size_t offset = 0;
        auto pull_bulk_ref = [](thallium::bulk& local,
                                size_t offset,
                                thallium::endpoint& ep,
                                const BulkRef& remote) {
            if(remote.size == 0) return;
            local(offset, remote.size) << remote.handle.on(ep)(remote.offset, remote.size);
        };
        // transfer metadata sizes
        thallium::endpoint remote_ep = m_engine.lookup(remote_meta_sizes.address);
        pull_bulk_ref(local_bulk, offset, remote_ep, remote_meta_sizes);
        offset += segments[0].second;
        // transfer metadata
        if(remote_meta_buffer.address != remote_meta_sizes.address)
            remote_ep = m_engine.lookup(remote_meta_buffer.address);
        pull_bulk_ref(local_bulk, offset, remote_ep, remote_meta_buffer);
        offset += segments[1].second;
        // transfer data descriptor sizes
        if(remote_desc_sizes.address != remote_meta_buffer.address)
            remote_ep = m_engine.lookup(remote_desc_sizes.address);
        pull_bulk_ref(local_bulk, offset, remote_ep, remote_desc_sizes);
        offset += segments[2].second;
        // transfer data descriptors
        if(remote_desc_buffer.address != remote_desc_sizes.address)
            remote_ep = m_engine.lookup(remote_desc_buffer.address);
        pull_bulk_ref(local_bulk, offset, remote_ep, remote_desc_buffer);
    }

    size_t count() const {
        return m_meta_sizes.size();
    }
};

class ActiveConsumerBatchQueue {

    public:
#if 0
    ActiveConsumerBatchQueue(
        std::string topic_name,
        std::string producer_name,
        std::shared_ptr<ClientImpl> client,
        std::shared_ptr<PartitionTargetInfoImpl> target,
        ThreadPool thread_pool,
        BatchSize batch_size)
    : m_topic_name(std::move(topic_name))
    , m_producer_name(std::move(producer_name))
    , m_client(std::move(client))
    , m_target(std::move(target))
    , m_thread_pool{std::move(thread_pool)}
    , m_batch_size{batch_size} {
        start();
    }

    ~ActiveConsumerBatchQueue() {
        stop();
    }

    void push(
            const Metadata& metadata,
            const Serializer& serializer,
            const Data& data,
            Promise<EventID> promise) {
        bool need_notification;
        {
            auto adaptive = m_batch_size == BatchSize::Adaptive();
            need_notification = adaptive;
            std::unique_lock<thallium::mutex> guard{m_mutex};
            if(m_batch_queue.empty())
                m_batch_queue.push(std::make_shared<ConsumerBatchImpl>());
            auto last_batch = m_batch_queue.back();
            if(!adaptive && last_batch->count() == m_batch_size.value) {
                m_batch_queue.push(std::make_shared<ConsumerBatchImpl>());
                last_batch = m_batch_queue.back();
                need_notification = true;
            }
            last_batch->push(metadata, serializer, data, std::move(promise));
        }
        if(need_notification)
            m_cv.notify_one();
    }

    void stop() {
        if(!m_running) return;
        {
            std::unique_lock<thallium::mutex> guard{m_mutex};
            m_need_stop = true;
        }
        m_cv.notify_one();
        m_terminated.wait();
        m_terminated.reset();
    }

    void start() {
        if(m_running) return;
        m_thread_pool.self->pushWork([this]() { loop(); });
    }

    void flush() {
        if(!m_running) return;
        {
            std::unique_lock<thallium::mutex> guard{m_mutex};
            m_request_flush = true;
        }
        m_cv.notify_one();
    }

    private:

    void loop() {
        m_running = true;
        std::unique_lock<thallium::mutex> guard{m_mutex};
        while(!m_need_stop || !m_batch_queue.empty()) {
            m_cv.wait(guard, [this]() {
                if(m_need_stop || m_request_flush)        return true;
                if(m_batch_queue.empty())                 return false;
                if(m_batch_size == BatchSize::Adaptive()) return true;
                auto batch = m_batch_queue.front();
                if(batch->count() == m_batch_size.value)  return true;
                return false;
            });
            if(m_batch_queue.empty()) {
                m_request_flush = false;
                continue;
            }
            auto batch = m_batch_queue.front();
            m_batch_queue.pop();
            guard.unlock();
            sendBatch(batch);
            guard.lock();
            m_request_flush = false;
        }
        m_running = false;
        m_terminated.set_value();
    }

    void sendBatch(const std::shared_ptr<ConsumerBatchImpl>& batch) {
        thallium::bulk metadata_content, data_content;
        try {
            metadata_content = batch->exposeMetadata(m_client->m_engine);
            data_content = batch->exposeData(m_client->m_engine);
        } catch(const std::exception& ex) {
            batch->setPromises(
                Exception{fmt::format(
                    "Unexpected error when registering batch for RDMA: {}", ex.what())});
            return;
        }
        try {
            auto ph = m_target->m_ph;
            auto rpc = m_client->m_producer_send_batch;
            auto self_addr = static_cast<std::string>(m_client->m_engine.self());
            RequestResult<EventID> result = rpc.on(ph)(
                m_topic_name,
                m_producer_name,
                batch->count(),
                BulkRef{metadata_content, 0, batch->metadataBulkSize(), self_addr},
                BulkRef{data_content, 0, batch->dataBulkSize(), self_addr});
            if(result.success())
                batch->setPromises(result.value());
            else
                batch->setPromises(Exception{result.error()});
        } catch(const std::exception& ex) {
            batch->setPromises(
                Exception{fmt::format("Unexpected error when sending batch: {}", ex.what())});
        }
    }

    std::string                                    m_topic_name;
    std::string                                    m_producer_name;
    std::shared_ptr<ClientImpl>                    m_client;
    std::shared_ptr<PartitionTargetInfoImpl>       m_target;
    ThreadPool                                     m_thread_pool;
    BatchSize                                      m_batch_size;
    std::queue<std::shared_ptr<ConsumerBatchImpl>> m_batch_queue;
    thallium::managed<thallium::thread>            m_sender_ult;
    bool                                           m_need_stop = false;
    bool                                           m_request_flush = false;
    std::atomic<bool>                              m_running = false;
    thallium::mutex                                m_mutex;
    thallium::condition_variable                   m_cv;
    thallium::eventual<void>                       m_terminated;
#endif
};

}

#endif
