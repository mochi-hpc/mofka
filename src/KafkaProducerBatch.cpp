/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "KafkaProducerBatch.hpp"
#include "KafkaBatchProducer.hpp"

namespace mofka {

void KafkaProducerBatch::push(
        Metadata metadata,
        Data data,
        Promise<EventID> promise) {
    Message msg{std::move(promise)};
    // make space at the beginning of the message to
    // store the size of the metadata
    msg.m_payload.resize(sizeof(size_t));
    // serialize the metadata
    BufferWrapperOutputArchive archive(msg.m_payload);
    m_serializer.serialize(archive, metadata);
    size_t metadata_size = msg.m_payload.size() - sizeof(size_t);
    std::memcpy(msg.m_payload.data(), &metadata_size, sizeof(metadata_size));
    // serialize the data, one segment at a time
    size_t offset = msg.m_payload.size();
    msg.m_payload.resize(msg.m_payload.size() + data.size());
    for(const auto& seg : data.segments()) {
        std::memcpy(msg.m_payload.data() + offset, seg.ptr, seg.size);
        offset += seg.size;
    }
    // push the message
    m_messages.push_back(std::move(msg));
}

void KafkaProducerBatch::send() {
    auto n = count();
    if(n == 0) return;
    // Create an array of messages to produce
    std::vector<rd_kafka_message_t> messages(n);
    for(size_t i = 0; i < n; ++i) {

        m_messages[i].m_on_delivered = [this,n,i](rd_kafka_t*, const rd_kafka_message_t* msg) {
            if(!msg->err) {
                m_messages[i].m_promise.setValue((EventID)msg->offset);
            } else {
                auto ex = Exception{
                    std::string{"Failed to send event: "} + rd_kafka_err2str(msg->err)};
                m_messages[i].m_promise.setException(std::move(ex));
            }
            if(++m_msg_sent == n)
                m_batch_sent.set_value();
        };
        messages[i].payload = m_messages[i].m_payload.data();
        messages[i].len     = m_messages[i].m_payload.size();
        messages[i].key = NULL;
        messages[i].key_len = 0;
        messages[i].partition = m_partition;
        messages[i]._private = static_cast<void*>(&m_messages[i].m_on_delivered);
    }
    // Produce the messages
    auto msg_count = rd_kafka_produce_batch(
            m_kafka_topic.get(), m_partition, 0, messages.data(), n);
    if ((unsigned long)msg_count != n) {
        for(size_t i = msg_count; i < n; ++i) {
            throw Exception{std::string{"Failed to produce all messages: "}
                + rd_kafka_err2str(messages[i].err)};
        }
    }
    // increase the number of pending batches
    {
        std::unique_lock<tl::mutex> pending_gard{m_owner->m_num_pending_batches_mtx};
        m_owner->m_num_pending_batches += 1;
    }
    // notify the polling ULT
    m_owner->m_num_pending_batches_cv.notify_one();
    // Wait for delivery report
    m_batch_sent.wait();
    // decrease the number of pending batches
    {
        std::unique_lock<tl::mutex> pending_gard{m_owner->m_num_pending_batches_mtx};
        m_owner->m_num_pending_batches += 1;
    }
    m_msg_sent = 0;
    m_batch_sent.reset();
    m_messages.clear();
}

}
