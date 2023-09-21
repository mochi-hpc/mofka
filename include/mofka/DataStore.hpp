/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_DATA_STORE_HPP
#define MOFKA_DATA_STORE_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/Metadata.hpp>
#include <mofka/DataDescriptor.hpp>
#include <mofka/RequestResult.hpp>
#include <mofka/EventID.hpp>
#include <mofka/BulkRef.hpp>
#include <mofka/Factory.hpp>

#include <thallium.hpp>

#include <vector>


namespace mofka {

/**
 * @brief Interface for a data store. To build a new data store,
 * implement a class MyDataStore that inherits from DataStore, and put
 * MOFKA_REGISTER_DATASTORE(mydatastore, MyDataStore); in a cpp file
 * that includes your backend class' header file.
 */
class DataStore {

    public:

    /**
     * @brief Constructor.
     */
    DataStore() = default;

    /**
     * @brief Move-constructor.
     */
    DataStore(DataStore&&) = default;

    /**
     * @brief Copy-constructor.
     */
    DataStore(const DataStore&) = default;

    /**
     * @brief Move-assignment operator.
     */
    DataStore& operator=(DataStore&&) = default;

    /**
     * @brief Copy-assignment operator.
     */
    DataStore& operator=(const DataStore&) = default;

    /**
     * @brief Destructor.
     */
    virtual ~DataStore() = default;

    /**
     * @brief Store a set of data pieces into the DataStore.
     *
     * @param count Number of data pieces to store.
     * @param sizes Bulk handle representing the list of sizes.
     * @param data Bulk handle containing the data.
     *
     * @return a vector of corresponding DataDescriptors.
     */
    virtual RequestResult<std::vector<DataDescriptor>> store(
        size_t count,
        const BulkRef& sizes,
        const BulkRef& data) = 0;


    /**
     * @brief Fetch the data associated with a given series of DataDescriptors.
     *
     * @param descriptors Vector of DataDescriptor for the data to fetch.
     * @param dest Bulk handle of the sender's memory.
     */
    virtual RequestResult<void> load(
        const std::vector<DataDescriptor>& descriptors,
        const BulkRef& dest) = 0;

    /**
     * @brief Destroys the DataStore, including the data it contains.
     *
     * @return a RequestResult<bool> instance indicating
     * whether the database was successfully destroyed.
     */
    virtual RequestResult<bool> destroy() = 0;

};

using DataStoreFactory = Factory<DataStore, const thallium::engine&, const Metadata&>;

#define MOFKA_REGISTER_DATASTORE(__name__, __type__) \
    MOFKA_REGISTER_IMPLEMENTATION_FOR(DataStoreFactory, __type__, __name__)

} // namespace mofka

#endif
