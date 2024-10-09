/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PRODUCER_BATCH_IFACE_H
#define MOFKA_PRODUCER_BATCH_IFACE_H

#include "mofka/Result.hpp"
#include "mofka/EventID.hpp"
#include "mofka/Metadata.hpp"
#include "mofka/Archive.hpp"
#include "mofka/Serializer.hpp"
#include "mofka/Data.hpp"
#include "mofka/Future.hpp"
#include "mofka/Producer.hpp"
#include "Promise.hpp"

namespace mofka {

class ProducerBatchInterface {

    public:

    virtual ~ProducerBatchInterface() = default;

    virtual void push(
            const Metadata& metadata,
            const Serializer& serializer,
            const Data& data,
            Promise<EventID> promise) = 0;

    virtual void send() = 0;

    virtual size_t count() const  = 0;

};

}

#endif
