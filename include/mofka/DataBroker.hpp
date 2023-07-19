/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_DATA_BROKER_HPP
#define MOFKA_DATA_BROKER_HPP

#include <mofka/Metadata.hpp>
#include <mofka/Data.hpp>
#include <functional>
#include <exception>
#include <stdexcept>

namespace mofka {

/**
 * @brief The DataBrokerInterface is the interface for an object
 * passed to a Consumer to tell it where to place the data associated
 * to each event it receives.
 *
 * An object inheriting from this class must provide a copy-constructor,
 * as the Consumer object will take a copy of it.
 */
class DataBrokerInterface {

    public:

    /**
     * @brief Constructor.
     */
    DataBrokerInterface() = default;

    /**
     * @brief Move constructor.
     */
    DataBrokerInterface(DataBrokerInterface&&) = default;

    /**
     * @brief Copy constructor.
     */
    DataBrokerInterface(const DataBrokerInterface&) = default;

    /**
     * @brief Move-assignment operator.
     */
    DataBrokerInterface& operator=(DataBrokerInterface&&) = default;

    /**
     * @brief Copy-assignment operator.
     */
    DataBrokerInterface& operator=(const DataBrokerInterface&) = default;

    /**
     * @brief Destructor.
     */
    virtual ~DataBrokerInterface() = default;

    /**
     * @see DataBroker::expose
     */
    virtual Data expose(const Metadata& metadata, size_t size) = 0;

    /**
     * @see DataBroker::release
     */
    virtual void release(const Metadata& metadata, const Data& data) = 0;

};

class DataBroker {

    public:

    /**
     * @brief Constructor.
     */
    template<typename InterfaceType>
    DataBroker(const InterfaceType& impl)
    : DataBroker(std::make_shared<InterfaceType>(impl)) {}

    /**
     * @brief Copy-constructor.
     */
    DataBroker(const DataBroker&);

    /**
     * @brief Move-constructor.
     */
    DataBroker(DataBroker&&);

    /**
     * @brief copy-assignment operator.
     */
    DataBroker& operator=(const DataBroker&);

    /**
     * @brief Move-assignment operator.
     */
    DataBroker& operator=(DataBroker&&);

    /**
     * @brief Destructor.
     */
    ~DataBroker();

    /**
     * @brief Return a Data instance pointing to a segment or a
     * set of segments of memory where data associated with an
     * event can be placed.
     *
     * @param metadata Metadata of the event.
     * @param size Expected total size of the Data.
     *
     * @return a Data instance.
     */
    Data expose(const Metadata& metadata, size_t size);

    /**
     * @brief Release the memory pointed to by the Data object,
     * i.e. tell the DataBroker that Mofka won't access the data
     * anymore past this point.
     *
     * @param metadata Metadata of the event.
     * @param data Data to release.
     */
    void release(const Metadata& metadata, const Data& data);

    /**
     * @brief Checks for the validity of the underlying pointer.
     */
    operator bool() const;

    private:

    std::shared_ptr<DataBrokerInterface> self;

    DataBroker(const std::shared_ptr<DataBrokerInterface>& impl);
};

}

#endif
