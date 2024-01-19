/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_EVENT_HPP
#define MOFKA_EVENT_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/Data.hpp>
#include <mofka/Metadata.hpp>
#include <mofka/Exception.hpp>
#include <mofka/PartitionSelector.hpp>
#include <mofka/EventID.hpp>

#include <memory>
#include <vector>

namespace mofka {

class EventImpl;
class ConsumerImpl;

/**
 * @brief An Event object encapsultes Metadata and Event, as well
 * as internal information about the origin of the event, and
 * enables consumers to acknowledge the event.
 */
class Event {

    friend class ConsumerImpl;

    public:

    /**
     * @brief Constructor leading to an invalid Event.
     */
    Event();

    /**
     * @brief Move-constructor.
     */
    Event(Event&&);

    /**
     * @brief Copy-constructor.
     */
    Event(const Event&);

    /**
     * @brief Copy-assignment operator.
     */
    Event& operator=(const Event&);

    /**
     * @brief Move-assignment operator.
     */
    Event& operator=(Event&&);

    /**
     * @brief Destructor.
     */
    ~Event();

    /**
     * @brief Get event Event's Metadata.
     */
    Metadata metadata() const;

    /**
     * @brief Get event Event's Data.
     */
    Data data() const;

    /**
     * @brief Returns information about the partition
     * this Event originates from.
     */
    PartitionInfo partition() const;

    /**
     * @brief Returns the EventID.
     */
    EventID id() const;

    /**
     * @brief Send a message to the provider to acknowledge the event.
     * Consumers will always restart reading events from the latest
     * acknowledged event in a partition.
     */
    void acknowledge() const;

    /**
     * @brief Checks if the Event instance is valid.
     */
    operator bool() const;

    private:

    /**
     * @brief Constructor is private. Use one of the static functions
     * to create a valid Event object.
     *
     * @param impl Pointer to implementation.
     */
    Event(const std::shared_ptr<EventImpl>& impl);

    std::shared_ptr<EventImpl> self;
};

}

#endif
