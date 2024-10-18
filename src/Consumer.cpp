/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Consumer.hpp"
#include "mofka/Result.hpp"
#include "mofka/Exception.hpp"
#include "mofka/TopicHandle.hpp"
#include "mofka/Future.hpp"

#include <limits>

using namespace std::string_literals;

namespace mofka {

Consumer::~Consumer() {
    if(self.use_count() == 1)
        self->unsubscribe();
}

TopicHandle Consumer::topic() const {
    return self->topic();
}

void Consumer::process(EventProcessor processor,
                       ThreadPool threadPool,
                       NumEvents maxEvents) const {

    // TODO
}

NumEvents NumEvents::Infinity() {
    return NumEvents{std::numeric_limits<size_t>::max()};
}

}
