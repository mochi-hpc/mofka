/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Producer.hpp"
#include "mofka/RequestResult.hpp"
#include "mofka/Exception.hpp"
#include "mofka/TopicHandle.hpp"

#include "AsyncRequestImpl.hpp"
#include "FutureImpl.hpp"
#include "ClientImpl.hpp"
#include "ProducerImpl.hpp"
#include "PimplUtil.hpp"

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS(Producer);

const std::string& Producer::name() const {
    return self->m_name;
}

ProducerOptions Producer::options() const {
    return self->m_options;
}

TopicHandle Producer::topic() const {
    return TopicHandle(self->m_topic);
}

Future<EventID> Producer::push(Metadata metadata, Data data) const {
    // TODO
    return Future<EventID>(
        std::make_shared<FutureImpl>(),
        [](std::shared_ptr<FutureImpl>) -> EventID { return 0; },
        [](std::shared_ptr<FutureImpl>) -> bool { return true; });
}

}
