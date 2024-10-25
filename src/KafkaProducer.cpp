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
            int timeout = m_thread_pool.size() > 1 ? 0 : 100;
            rd_kafka_poll(m_kafka_producer.get(), timeout);
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
        m_poll_ult_stopped.wait();
    }
}

struct Message {
    Promise<EventID>                                            promise;
    std::vector<char>                                           serialized_metadata;
    size_t                                                      serialized_metadata_size;
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

        /* serialize the metadata */
        BufferWrapperOutputArchive archive{msg->serialized_metadata};
        m_topic->serializer().serialize(archive, metadata);
        msg->serialized_metadata_size = msg->serialized_metadata.size();
        msg->promise = promise;

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
        std::vector<rd_kafka_vu_t> args(data.segments().size() + 5);
        args[0].vtype      = RD_KAFKA_VTYPE_RKT;
        args[0].u.rkt      = m_kafka_topic.get();
        args[1].vtype      = RD_KAFKA_VTYPE_PARTITION;
        args[1].u.i32      = (int32_t)partition_index;
        args[2].vtype      = RD_KAFKA_VTYPE_OPAQUE;
        args[2].u.ptr      = &msg->on_delivery;
        args[3].vtype      = RD_KAFKA_VTYPE_VALUE;
        args[3].u.mem.ptr  = &msg->serialized_metadata_size;
        args[3].u.mem.size = sizeof(msg->serialized_metadata_size);
        args[4].vtype      = RD_KAFKA_VTYPE_VALUE;
        args[4].u.mem.ptr  = msg->serialized_metadata.data();
        args[4].u.mem.size = msg->serialized_metadata.size();
        for(size_t i = 0; i < data.segments().size(); ++i) {
            args[5+i].vtype      = RD_KAFKA_VTYPE_VALUE;
            args[5+i].u.mem.ptr  = data.segments()[i].ptr;
            args[5+i].u.mem.size = data.segments()[i].size;
        }

        m_num_pending_messages++;
        auto err = rd_kafka_produceva(m_kafka_producer.get(), args.data(), args.size());
        if(err) {
            m_num_pending_messages--;
            auto err_str = std::string{rd_kafka_error_string(err)};
            rd_kafka_error_destroy(err);
            delete msg;
            throw Exception{"Failed to produce all messages: " + err_str};
        }

    } catch(const Exception& ex) {
        promise.setException(ex);
    }

    return future;
}

void KafkaProducer::flush() {
    while(m_num_pending_messages != 0) {
        rd_kafka_flush(m_kafka_producer.get(), 100);
        tl::thread::yield();
    }
}

}
