/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_SERVICE_HANDLE_HPP
#define MOFKA_SERVICE_HANDLE_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/Client.hpp>
#include <mofka/Exception.hpp>
#include <mofka/Serializer.hpp>
#include <mofka/Validator.hpp>
#include <mofka/PartitionSelector.hpp>
#include <mofka/Metadata.hpp>

#include <thallium.hpp>
#include <rapidjson/document.h>
#include <memory>
#include <unordered_set>

namespace mofka {

class ServiceHandleImpl;

/**
 * @brief A ServiceHandle object is a handle for a Mofka service
 * deployed on a set of servers.
 */
class ServiceHandle {

    friend class Client;
    friend class TopicHandle;

    public:

    /**
     * @brief Constructor. The resulting ServiceHandle handle will be invalid.
     */
    ServiceHandle();

    /**
     * @brief Copy-constructor.
     */
    ServiceHandle(const ServiceHandle&);

    /**
     * @brief Move-constructor.
     */
    ServiceHandle(ServiceHandle&&);

    /**
     * @brief Copy-assignment operator.
     */
    ServiceHandle& operator=(const ServiceHandle&);

    /**
     * @brief Move-assignment operator.
     */
    ServiceHandle& operator=(ServiceHandle&&);

    /**
     * @brief Destructor.
     */
    ~ServiceHandle();

    /**
     * @brief Returns the client this database has been opened with.
     */
    Client client() const;

    /**
     * @brief Checks if the ServiceHandle instance is valid.
     */
    operator bool() const;

    /**
     * @brief Returns the number of servers this service currently has.
     */
    size_t numServers() const;

    /**
     * @brief Create a topic with a given name, if it does not exist yet.
     *
     * @param name Name of the topic.
     * @param validator Validator object to validate events pushed to the topic.
     * @param selector PartitionSelector object of the topic.
     * @param serializer Serializer to use for all the events in the topic.
     */
    void createTopic(std::string_view name,
                     Validator validator = Validator{},
                     PartitionSelector selector = PartitionSelector{},
                     Serializer serializer = Serializer{});

    /**
     * @brief Open an existing topic with the given name.
     *
     * @param name Name of the topic.
     *
     * @return a TopicHandle representing the topic.
     */
    TopicHandle openTopic(std::string_view name);

    /**
     * @brief Map of dependency descriptors for the partition.
     * Please refer to the partition type's documentation for more information
     * on its expected dependencies.
     */
    typedef std::unordered_map<std::string, std::vector<std::string>> PartitionDependencies;

    /**
     * @brief Create a new partition at the given server rank and add it to the topic.
     *
     * @param topic_name Name of the topic.
     * @param server_rank Rank of the server.
     * @param partition_type Type of partition.
     * @param partition_config Configuration for the partition.
     * @param dependencies Map of dependencies expected by the partition.
     * @param pool_name Pool name in the server.
     */
    void addPartition(std::string_view topic_name,
                      size_t server_rank,
                      std::string_view partition_type = "memory",
                      const Metadata& partition_config = Metadata{"{}"},
                      const PartitionDependencies& dependencies = {},
                      std::string_view pool_name = "");

    private:

    /**
     * @brief Constructor is private. Use a Client object
     * to create a ServiceHandle instance.
     *
     * @param impl Pointer to implementation.
     */
    ServiceHandle(const std::shared_ptr<ServiceHandleImpl>& impl);

    std::shared_ptr<ServiceHandleImpl> self;
};

}

#endif
