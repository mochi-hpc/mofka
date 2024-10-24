/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Producer.hpp"
#include "mofka/Exception.hpp"
#include "mofka/TopicHandle.hpp"

#include "Promise.hpp"
#include "BatchProducer.hpp"
#include "ActiveProducerBatchQueue.hpp"

namespace mofka {

TopicHandle BatchProducer::topic() const {
    return m_topic;
}

Future<EventID> BatchProducer::push(Metadata metadata, Data data) {
    /* Step 1: create a future/promise pair for this operation */
    Future<EventID> future;
    Promise<EventID> promise;
    // if the batch size is not adaptive, wait() calls on futures should trigger a flush
    auto on_wait = [this]() mutable { flush(); };
    std::tie(future, promise) = m_batch_size != BatchSize::Adaptive() ?
        Promise<EventID>::CreateFutureAndPromise(std::move(on_wait))
        : Promise<EventID>::CreateFutureAndPromise();
    /* Step 2: get a local ID for this push operation */
    size_t local_event_id = m_num_pushed_events++;
    /* Step 3: create a ULT that will validate, select the partition, and serialize */
    auto ult = [this,
                local_event_id,
                promise=std::move(promise),
                metadata=std::move(metadata),
                data=std::move(data)]() mutable {
        auto topic = m_topic;
        /* Metadata validation */
        try {
            /* Step 3.1: validate the metadata */
            topic.validator().validate(metadata, data);
            /* Step 3.2: select the partition for this metadata */
            auto partition_index = topic.selector().selectPartitionFor(metadata);
            {
                /* Step 3.3: wait for our turn pushing the event into the batch */
                std::unique_lock<thallium::mutex> guard{m_batch_queues_mtx};
                if(m_ordering == Ordering::Strict) {
                    while(local_event_id != m_num_ready_events) {
                        m_batch_queues_cv.wait(guard);
                    }
                }
                /* Step 3.4: find/create the ActiveBatchQueue to send to */
                auto queue_it = m_batch_queues.find(partition_index);
                decltype(queue_it->second) queue;
                if(queue_it == m_batch_queues.end()) {
                    auto create_new_batch = [this, partition_index]() {
                        return newBatchForPartition(partition_index);
                    };
                    queue = std::make_shared<ActiveProducerBatchQueue>(
                            create_new_batch,
                            m_thread_pool,
                            batchSize());
                    m_batch_queues[partition_index] = queue;
                } else {
                    queue = queue_it->second;
                }
                if(m_ordering != Ordering::Strict)
                    guard.unlock();
                /* Step 3.5: push the data and metadata to the batch */
                queue->push(metadata, topic.serializer(), data, promise);
                /* Step 3.6: increase the number of events that are ready */
                m_num_ready_events += 1;
                /* Step 3.7: now the ActiveBatchQueue ULT will automatically
                 * pick up the batch and send it when needed */
            }
        } catch(const Exception& ex) {
            /* Increase the number of events that are ready
             * (because it wasn't done in the normal path) */
            m_num_ready_events += 1;
            promise.setException(ex);
        }
        /* Step 3.8: notify ULTs blocked waiting for their turn */
        m_batch_queues_cv.notify_all();
        /* Step 3.9: decrease the number of posted ULTs */
        bool notify_no_posted_ults = false;
        {
            std::lock_guard<thallium::mutex> guard_posted_ults{m_num_posted_ults_mtx};
            m_num_posted_ults -= 1;
            if(m_num_posted_ults == 0) notify_no_posted_ults = true;
        }
        if(notify_no_posted_ults) {
            m_num_posted_ults_cv.notify_all();
        }
    };
    /* Step 4: increase the number of posted ULTs */
    {
        std::lock_guard<thallium::mutex> guard{m_num_posted_ults_mtx};
        m_num_posted_ults += 1;
    }
    /* Step 3: submit the ULT */
    m_thread_pool.pushWork(std::move(ult), local_event_id);
    /* Step 4: return the future */
    return future;
}

void BatchProducer::flush() {
    {
        std::unique_lock<thallium::mutex> guard_posted_ults{m_num_posted_ults_mtx};
        m_num_posted_ults_cv.wait(
                guard_posted_ults,
                [this]() { return m_num_posted_ults == 0; });
    }
    {
        std::lock_guard<thallium::mutex> guard{m_batch_queues_mtx};
        for(auto& p : m_batch_queues) {
            p.second->flush();
        }
    }
}

}
