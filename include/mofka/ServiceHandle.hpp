/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_SERVICE_HANDLE_HPP
#define MOFKA_SERVICE_HANDLE_HPP

#include <thallium.hpp>
#include <rapidjson/document.h>
#include <mofka/Client.hpp>
#include <mofka/Exception.hpp>
#include <mofka/AsyncRequest.hpp>
#include <mofka/Serializer.hpp>
#include <mofka/Metadata.hpp>
#include <memory>
#include <unordered_set>

namespace mofka {

class Client;
class ServiceHandleImpl;
class TopicHandle;

struct TopicBackendConfig : public Metadata {

    template<typename ... Args>
    TopicBackendConfig(Args&&... args)
    : Metadata(std::forward<Args>(args)...) {}

};

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
     * @brief Create a topic with a given name, if it does not exist yet.
     *
     * @param name Name of the topic.
     * @param config Json configuration of the topic's backend.
     * @param serializer Serializer to use for all the events in the topic.
     *
     * @return a TopicHandle representing the topic.
     */
    TopicHandle createTopic(std::string_view name,
                            TopicBackendConfig config = TopicBackendConfig{},
                            Serializer serializer = Serializer{});

    /**
     * @brief Open an existing topic with the given name.
     *
     * @param name Name of the topic.
     *
     * @return a TopicHandle representing the topic.
     */
    TopicHandle openTopic(std::string_view name);

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
