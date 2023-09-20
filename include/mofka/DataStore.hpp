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

#include <thallium.hpp>

#include <vector>

/**
 * @brief Helper class to register backend types into the data store factory.
 */
template<typename DataStoreType>
class __MofkaDataStoreRegistration;

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

/**
 * @brief The DataStoreFactory contains functions to create DataStore objects.
 */
class DataStoreFactory {

    template<typename DataStoreType>
    friend class ::__MofkaDataStoreRegistration;

    public:

    DataStoreFactory() = delete;

    /**
     * @brief Creates a DataStore and returns a unique_ptr to the created instance.
     *
     * @param backend_name Name of the backend to use.
     * @param engine Thallium engine.
     * @param config Configuration to pass to the backend's create function.
     *
     * @return a unique_ptr to the created DataStore.
     */
    static std::unique_ptr<DataStore> createDataStore(
            std::string_view backend_name,
            const thallium::engine& engine,
            const Metadata& config);

    private:

    static std::unordered_map<std::string,
                std::function<std::unique_ptr<DataStore>(
                    const thallium::engine&,
                    const Metadata&)>> create_fn;
};

} // namespace mofka


#define MOFKA_REGISTER_DATASTORE(__backend_name, __backend_type) \
    static __MofkaDataStoreRegistration<__backend_type> __mofka ## __backend_name ## _storage( #__backend_name )

template<typename DataStoreType>
class __MofkaDataStoreRegistration {

    public:

    __MofkaDataStoreRegistration(std::string_view backend_name)
    {
        mofka::DataStoreFactory::create_fn.emplace(
            backend_name, [](const thallium::engine& engine,
                             const mofka::Metadata& config) {
                return DataStoreType::create(engine, config);
            });
    }
};

#endif
