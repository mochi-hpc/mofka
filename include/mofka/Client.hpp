/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_CLIENT_HPP
#define MOFKA_CLIENT_HPP

#include <mofka/ServiceHandle.hpp>
#include <mofka/UUID.hpp>
#include <mofka/Json.hpp>
#include <thallium.hpp>
#include <memory>
#include <string_view>

namespace mofka {

class ClientImpl;
class ServiceHandle;

/**
 * @brief SSGFileName is a string_view representing the name
 * of an SSG group file.
 */
struct SSGFileName : public std::string_view {
    template<typename ... Args>
    explicit SSGFileName(Args&&... args)
    : std::string_view(std::forward<Args>(args)...) {}
};

/**
 * @brief The SSGGroupID is a wrapper for an ssg_group_id_t
 * (without having to rely on including SSG).
 */
struct SSGGroupID {
    uint64_t value;
    explicit SSGGroupID(uint64_t v) : value(v) {}
};

/**
 * @brief The Client object is the main object used to establish
 * a connection with a Mofka service.
 */
class Client {

    friend class ServiceHandle;

    public:

    /**
     * @brief Default constructor.
     */
    Client();

    /**
     * @brief Constructor.
     *
     * @param engine Thallium engine.
     */
    Client(const thallium::engine& engine);

    /**
     * @brief Copy constructor.
     */
    Client(const Client&);

    /**
     * @brief Move constructor.
     */
    Client(Client&&);

    /**
     * @brief Copy-assignment operator.
     */
    Client& operator=(const Client&);

    /**
     * @brief Move-assignment operator.
     */
    Client& operator=(Client&&);

    /**
     * @brief Destructor.
     */
    ~Client();

    /**
     * @brief Returns the thallium engine used by the client.
     */
    const thallium::engine& engine() const;

    /**
     * @brief Creates a ServiceHandle representing a Mofka service.
     *
     * @param filename SSG group file name of the service.
     *
     * @return a ServiceHandle instance.
     */
    ServiceHandle connect(SSGFileName ssgfile) const;

    /**
     * @brief Creates a ServiceHandle representing a Mofka service.
     *
     * @param group_id SSG group id.
     *
     * @return a ServiceHandle instance.
     */
    ServiceHandle connect(SSGGroupID gid) const;

    /**
     * @brief Checks that the Client instance is valid.
     */
    operator bool() const;

    /**
     * @brief Get internal configuration.
     *
     * @return configuration.
     */
    const rapidjson::Value& getConfig() const;

    private:

    Client(const std::shared_ptr<ClientImpl>& impl);

    std::shared_ptr<ClientImpl> self;
};

}

#endif
