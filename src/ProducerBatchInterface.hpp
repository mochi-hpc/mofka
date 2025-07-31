/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PRODUCER_BATCH_IFACE_H
#define MOFKA_PRODUCER_BATCH_IFACE_H

#include <diaspora/EventID.hpp>
#include <diaspora/Metadata.hpp>
#include <diaspora/Archive.hpp>
#include <diaspora/Serializer.hpp>
#include <diaspora/DataView.hpp>
#include <diaspora/Future.hpp>
#include <diaspora/Producer.hpp>

#include "Promise.hpp"

namespace mofka {

class ProducerBatchInterface {

    public:

    virtual ~ProducerBatchInterface() = default;

    virtual void push(
            diaspora::Metadata metadata,
            diaspora::DataView data,
            Promise<diaspora::EventID> promise) = 0;

    virtual void send() = 0;

    virtual size_t count() const  = 0;

};

}

#endif
