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

/**
 * @brief The EventInterface is used by streaming drivers
 * to implement their Event.
 */
class EventInterface {

    public:

    /**
     * @brief Destructor.
     */
    virtual ~EventInterface() = default;

    /**
     * @brief Get an Event's Metadata.
     */
    virtual Metadata metadata() const = 0;

    /**
     * @brief Get an Event's Data.
     */
    virtual Data data() const = 0;

    /**
     * @brief Returns information about the partition
     * this Event originates from.
     */
    virtual PartitionInfo partition() const = 0;

    /**
     * @brief Returns the EventID.
     */
    virtual EventID id() const = 0;

    /**
     * @brief Acknowledge the event.
     * Consumers will always restart reading events from the latest
     * acknowledged event in a partition.
     */
    virtual void acknowledge() const = 0;

};

/**
 * @brief An Event object encapsultes Metadata and Event, as well
 * as internal information about the origin of the event, and
 * enables consumers to acknowledge the event.
 */
class Event {

    public:

    /**
     * @brief Constructor.
     */
    inline Event(std::shared_ptr<EventInterface> impl = nullptr)
    : self{std::move(impl)} {}

    /**
     * @brief Move-constructor.
     */
    Event(Event&&) = default;

    /**
     * @brief Copy-constructor.
     */
    Event(const Event&) = default;

    /**
     * @brief Copy-assignment operator.
     */
    Event& operator=(const Event&) = default;

    /**
     * @brief Move-assignment operator.
     */
    Event& operator=(Event&&) = default;

    /**
     * @brief Destructor.
     */
    ~Event() = default;

    /**
     * @brief Get event Event's Metadata.
     */
    inline Metadata metadata() const {
        return self->metadata();
    }

    /**
     * @brief Get event Event's Data.
     */
    inline Data data() const {
        return self->data();
    }

    /**
     * @brief Returns information about the partition
     * this Event originates from.
     */
    inline PartitionInfo partition() const {
        return self->partition();
    }

    /**
     * @brief Returns the EventID.
     */
    inline EventID id() const {
        return self->id();
    }

    /**
     * @brief Send a message to the provider to acknowledge the event.
     * Consumers will always restart reading events from the latest
     * acknowledged event in a partition.
     */
    inline void acknowledge() const {
        return self->acknowledge();
    }

    /**
     * @brief Checks if the Event instance is valid.
     */
    inline operator bool() const {
        return static_cast<bool>(self);
    }

    private:

    std::shared_ptr<EventInterface> self;
};

}

#endif
