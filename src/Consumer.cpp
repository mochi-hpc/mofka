/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Consumer.hpp"
#include "mofka/RequestResult.hpp"
#include "mofka/Exception.hpp"
#include "mofka/TopicHandle.hpp"
#include "mofka/Future.hpp"

#include "AsyncRequestImpl.hpp"
#include "Promise.hpp"
#include "ClientImpl.hpp"
#include "ConsumerImpl.hpp"
#include "PimplUtil.hpp"
#include "ThreadPoolImpl.hpp"
#include <limits>

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS(Consumer);

const std::string& Consumer::name() const {
    return self->m_name;
}

TopicHandle Consumer::topic() const {
    return TopicHandle(self->m_topic);
}

BatchSize Consumer::batchSize() const {
    return self->m_batch_size;
}

ThreadPool Consumer::threadPool() const {
    return self->m_thread_pool;
}

DataBroker Consumer::dataBroker() const {
    return self->m_data_broker;
}

DataSelector Consumer::dataSelector() const {
    return self->m_data_selector;
}

Future<Event> Consumer::pull() const {
    Future<Event> future;
    std::unique_lock<thallium::mutex> guard{self->m_futures_mtx};
    if(self->m_futures_credit || self->m_futures.empty()) {
        // the queue of futures is empty or the futures
        // already in the queue have been created by
        // previous calls to pull() that haven't completed
        Promise<Event> promise;
        std::tie(future, promise) = Promise<Event>::CreateFutureAndPromise();
        self->m_futures.emplace_back(std::move(promise), future);
        self->m_futures_credit = true;
    } else {
        // the queue of futures has futures already
        // created by the consumer
        Future<Event> future = std::move(self->m_futures.back().second);
        self->m_futures.pop_back();
    }
    return future;
}

void Consumer::process(EventProcessor processor,
                       ThreadPool threadPool,
                       NumEvents maxEvents) const {

    // TODO
}

void Consumer::operator|(EventProcessor processor) const && {
    process(processor, self->m_thread_pool, NumEvents::Infinity());
}

NumEvents NumEvents::Infinity() {
    return NumEvents{std::numeric_limits<size_t>::max()};
}

void ConsumerImpl::start() {
    // for each target, submit a ULT that pulls from that target
    auto n = m_targets.size();
    m_pulling_ult_completed.resize(n);
    for(size_t i=0; i < n; ++i) {
        m_thread_pool.self->pushWork(
            [this, i](){
                pullFrom(m_targets[i], m_pulling_ult_completed[i]);
        });
    }
}

void ConsumerImpl::join() {
    // wait for the ULTs to complete
    for(auto& ev : m_pulling_ult_completed)
        ev.wait();
}

void ConsumerImpl::pullFrom(const PartitionTargetInfo& target,
                            thallium::eventual<void>& ev) {
    // TODO
    ev.set_value();
}

}
