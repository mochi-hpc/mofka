/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_ACTIVE_PRODUCER_BATCH_QUEUE_H
#define MOFKA_ACTIVE_PRODUCER_BATCH_QUEUE_H

#include "Promise.hpp"
#include "DataImpl.hpp"
#include "PimplUtil.hpp"
#include "ProducerBatchInterface.hpp"

#include <thallium.hpp>
#include <mutex>
#include <queue>
#include <vector>
#include <cstdint>

namespace mofka {

namespace tl = thallium;

class ActiveProducerBatchQueue {

    public:

    using NewBatchFn = std::function<std::shared_ptr<ProducerBatchInterface>()>;

    ActiveProducerBatchQueue(
        NewBatchFn new_batch,
        ThreadPool thread_pool,
        BatchSize batch_size)
    : m_create_new_batch{std::move(new_batch)}
    , m_thread_pool{std::move(thread_pool)}
    , m_batch_size{batch_size} {
        start();
    }

    ~ActiveProducerBatchQueue() {
        stop();
    }

    void push(Metadata metadata,
              Data data,
              Promise<EventID> promise) {
        bool need_notification;
        {
            auto adaptive = m_batch_size == BatchSize::Adaptive();
            need_notification = adaptive;
            std::unique_lock<thallium::mutex> guard{m_mutex};
            if(m_batch_queue.empty())
                m_batch_queue.push(m_create_new_batch());
            auto last_batch = m_batch_queue.back();
            if(!adaptive && last_batch->count() == m_batch_size.value) {
                m_batch_queue.push(m_create_new_batch());
                last_batch = m_batch_queue.back();
                need_notification = true;
            }
            last_batch->push(std::move(metadata), std::move(data), std::move(promise));
        }
        if(need_notification) {
            m_cv.notify_one();
        }
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
        m_running = true;
        m_thread_pool.pushWork([this]() { loop(); });
    }

    void flush() {
        if(!m_running) return;
        {
            std::unique_lock<thallium::mutex> guard{m_mutex};
            m_request_flush = true;
            m_cv.notify_one();
            m_cv.wait(guard, [this]() { return m_batch_queue.empty(); });
        }
    }

    private:

    void loop() {
        std::unique_lock<thallium::mutex> guard{m_mutex};
        while(!m_need_stop || !m_batch_queue.empty()) {
            m_cv.wait(guard, [this]() {
                if(m_need_stop || m_request_flush)        return true;
                if(m_batch_queue.empty())                 return false;
                if(m_batch_size == BatchSize::Adaptive()) return true;
                auto& batch = m_batch_queue.front();
                if(batch->count() == m_batch_size.value)  return true;
                return false;
            });
            if(m_batch_queue.empty()) {
                if(m_request_flush) {
                    m_request_flush = false;
                    m_cv.notify_one();
                }
                continue;
            }
            auto batch = m_batch_queue.front();
            m_batch_queue.pop();
            guard.unlock();
            batch->send();
            guard.lock();
        }
        m_running = false;
        m_terminated.set_value();
    }

    NewBatchFn                             m_create_new_batch;
    ThreadPool                             m_thread_pool;
    BatchSize                              m_batch_size;
    std::queue<SP<ProducerBatchInterface>> m_batch_queue;
    thallium::managed<thallium::thread>    m_sender_ult;
    bool                                   m_need_stop = false;
    bool                                   m_request_flush = false;
    std::atomic<bool>                      m_running = false;
    thallium::mutex                        m_mutex;
    thallium::condition_variable           m_cv;
    thallium::eventual<void>               m_terminated;

};

}

#endif
