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
#include "KafkaProducer.hpp"
#include "ActiveProducerBatchQueue.hpp"
#include "PimplUtil.hpp"
#include <limits>

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>

namespace mofka {

KafkaProducer::KafkaProducer(
        std::string_view name,
        BatchSize batch_size,
        ThreadPool thread_pool,
        Ordering ordering,
        std::shared_ptr<KafkaTopicHandle> topic,
        std::shared_ptr<rd_kafka_t> kprod,
        std::shared_ptr<rd_kafka_topic_t> ktopic)
: m_name(name.data(), name.size())
, m_batch_size{batch_size}
, m_thread_pool{thread_pool}
, m_ordering{ordering}
, m_topic(std::move(topic))
, m_kafka_producer(std::move(kprod))
, m_kafka_topic(std::move(ktopic))
{
    start();
}

void KafkaProducer::start() {
    m_should_stop = false;
    auto run = [this](){
        while(!m_should_stop) {
            std::unique_lock<tl::mutex> pending_msg_guard{m_num_pending_messages_mtx};
            m_num_pending_messages_cv.wait(pending_msg_guard,
                [this](){ return m_should_stop || m_num_pending_messages > 0; });
            while(rd_kafka_poll(m_kafka_producer.get(), 0) != 0) {
                if(m_should_stop) break;
            }
            if(m_should_stop) break;
            tl::thread::yield();
        }
        m_poll_ult_stopped.set_value();
    };
    m_thread_pool.pushWork(std::move(run));
}

TopicHandle KafkaProducer::topic() const {
    return TopicHandle{m_topic};
}

KafkaProducer::~KafkaProducer() {
    flush();
    if(!m_should_stop) {
        m_should_stop = true;
        m_num_pending_messages_cv.notify_all();
        m_poll_ult_stopped.wait();
    }
}

struct Message {
    Promise<EventID>                                            promise;
    std::vector<char>                                           payload;
    std::function<void(rd_kafka_t*, const rd_kafka_message_t*)> on_delivery;
};

Future<EventID> KafkaProducer::push(Metadata metadata, Data data) {

    Future<EventID>  future;
    Promise<EventID> promise;

    auto on_wait = [this]() mutable { flush(); };
    std::tie(future, promise) = Promise<EventID>::CreateFutureAndPromise(std::move(on_wait));

    try {
        /* validate */
        m_topic->validator().validate(metadata, data);

        /* select partition */
        auto partition_index = m_topic->selector().selectPartitionFor(metadata);

        auto msg = new Message();
        msg->promise = promise;

        /* serialize the payload */
        msg->payload.resize(sizeof(size_t));
        BufferWrapperOutputArchive archive{msg->payload};
        m_topic->serializer().serialize(archive, metadata);
        size_t metadata_size = msg->payload.size() - sizeof(size_t);
        std::memcpy(msg->payload.data(), &metadata_size, sizeof(metadata_size));
        size_t offset = msg->payload.size();
        msg->payload.resize(offset + data.size());
        for(auto& seg : data.segments()) {
            std::memcpy(msg->payload.data() + offset, seg.ptr, seg.size);
            offset += seg.size;
        }

        msg->on_delivery = [msg,this](rd_kafka_t*, const rd_kafka_message_t* kmsg) {
            if(!kmsg->err) {
                 msg->promise.setValue((EventID)kmsg->offset);
            } else {
                auto ex = Exception{
                    std::string{"Failed to send event: "} + rd_kafka_err2str(kmsg->err)};
                msg->promise.setException(std::move(ex));
            }
            m_num_pending_messages--;
            delete msg;
        };

        /* send to kafka */
        std::vector<rd_kafka_vu_t> args(4);
        args[0].vtype      = RD_KAFKA_VTYPE_RKT;
        args[0].u.rkt      = m_kafka_topic.get();
        args[1].vtype      = RD_KAFKA_VTYPE_PARTITION;
        args[1].u.i32      = (int32_t)partition_index;
        args[2].vtype      = RD_KAFKA_VTYPE_OPAQUE;
        args[2].u.ptr      = &msg->on_delivery;
        args[3].vtype      = RD_KAFKA_VTYPE_VALUE;
        args[3].u.mem.ptr  = msg->payload.data();
        args[3].u.mem.size = msg->payload.size();

        bool notify = true;
        {
            std::unique_lock<tl::mutex> pending_msg_guard{m_num_pending_messages_mtx};
            m_num_pending_messages++;
        }
        auto err = rd_kafka_produceva(m_kafka_producer.get(), args.data(), args.size());
        if(err) {
            {
                std::unique_lock<tl::mutex> pending_msg_guard{m_num_pending_messages_mtx};
                m_num_pending_messages--;
                notify = false;
            }
            auto err_str = std::string{rd_kafka_error_string(err)};
            rd_kafka_error_destroy(err);
            delete msg;
            throw Exception{"Failed to produce all messages: " + err_str};
        }

        rd_kafka_poll(m_kafka_producer.get(), 0);

        if(notify) m_num_pending_messages_cv.notify_one();

    } catch(const Exception& ex) {
        promise.setException(ex);
    }

    return future;
}

void KafkaProducer::flush() {
    std::unique_lock<tl::mutex> pending_msg_guard{m_num_pending_messages_mtx};
    if(m_num_pending_messages)
        rd_kafka_flush(m_kafka_producer.get(), -1);
}

}
