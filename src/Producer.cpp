/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Producer.hpp"
#include "mofka/Result.hpp"
#include "mofka/Exception.hpp"
#include "mofka/TopicHandle.hpp"
#include "mofka/Future.hpp"

#include "Promise.hpp"
#include "ClientImpl.hpp"
#include "ProducerImpl.hpp"
#include "PimplUtil.hpp"
#include "ThreadPoolImpl.hpp"
#include <limits>

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS_NO_DTOR(Producer);

Producer::~Producer() {
    flush();
}

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
    // if the batch size is not adaptive, wait() calls on futures should trigger a flush
    auto on_wait = [producer=*this]() mutable { producer.flush(); };
    std::tie(future, promise) = self->m_batch_size != BatchSize::Adaptive() ?
        Promise<EventID>::CreateFutureAndPromise(std::move(on_wait))
        : Promise<EventID>::CreateFutureAndPromise();
    /* Step 2: get a local ID for this push operation */
    size_t local_event_id = self->m_num_pushed_events++;
    /* Step 3: create a ULT that will validate, select the partition, and serialize */
    auto ult = [this,
                local_event_id,
                promise=std::move(promise),
                metadata=std::move(metadata),
                data=std::move(data)]() mutable {
        auto topic = self->m_topic;
        /* Metadata validation */
        try {
            /* Step 3.1: validate the metadata */
            topic->m_validator.validate(metadata, data);
            /* Step 3.2: select the partition for this metadata */
            auto partition = topic->m_selector.selectPartitionFor(metadata);
            {
                /* Step 3.3: wait for our turn pushing the event into the batch */
                std::unique_lock<thallium::mutex> guard{self->m_batch_queues_mtx};
                if(self->m_ordering == Ordering::Strict) {
                    while(local_event_id != self->m_num_ready_events) {
                        self->m_batch_queues_cv.wait(guard);
                    }
                }
                /* Step 3.4: find/create the ActiveBatchQueue to send to */
                auto& queue = self->m_batch_queues[partition];
                if(!queue) {
                    queue.reset(new ActiveProducerBatchQueue{
                        self->m_name,
                        self->m_topic->m_service->m_client,
                        partition.self,
                        self->m_thread_pool,
                        batchSize()});
                }
                if(self->m_ordering != Ordering::Strict)
                    guard.unlock();
                /* Step 3.5: push the data and metadata to the batch */
                queue->push(metadata, topic->m_serializer, data, promise);
                /* Step 3.6: increase the number of events that are ready */
                self->m_num_ready_events += 1;
                /* Step 3.7: now the ActiveBatchQueue ULT will automatically
                 * pick up the batch and send it when needed */
            }
        } catch(const Exception& ex) {
            /* Increase the number of events that are ready
             * (because it wasn't done in the normal path) */
            self->m_num_ready_events += 1;
            promise.setException(ex);
        }
        /* Step 3.8: notify ULTs blocked waiting for their turn */
        self->m_batch_queues_cv.notify_all();
        /* Step 3.9: decrease the number of posted ULTs */
        bool notify_no_posted_ults = false;
        {
            std::lock_guard<thallium::mutex> guard_posted_ults{self->m_num_posted_ults_mtx};
            self->m_num_posted_ults -= 1;
            if(self->m_num_posted_ults == 0) notify_no_posted_ults = true;
        }
        if(notify_no_posted_ults) {
            self->m_num_posted_ults_cv.notify_all();
        }
    };
    /* Step 4: increase the number of posted ULTs */
    {
        std::lock_guard<thallium::mutex> guard{self->m_num_posted_ults_mtx};
        self->m_num_posted_ults += 1;
    }
    /* Step 3: submit the ULT */
    self->m_thread_pool->pushWork(std::move(ult), local_event_id);
    /* Step 4: return the future */
    return future;
}

void Producer::flush() {
    if(!self) return;
    {
        std::unique_lock<thallium::mutex> guard_posted_ults{self->m_num_posted_ults_mtx};
        self->m_num_posted_ults_cv.wait(
            guard_posted_ults,
            [this]() { return self->m_num_posted_ults == 0; });
    }
    {
        std::lock_guard<thallium::mutex> guard{self->m_batch_queues_mtx};
        for(auto& p : self->m_batch_queues) {
            p.second->flush();
        }
    }
}

BatchSize BatchSize::Adaptive() {
    return BatchSize{std::numeric_limits<std::size_t>::max()};
}

}
