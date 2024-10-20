/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_CLIENT_HPP
#define MOFKA_CLIENT_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/MofkaDriver.hpp>
#include <mofka/UUID.hpp>
#include <mofka/Json.hpp>

#include <thallium.hpp>
#include <memory>
#include <string_view>

namespace mofka {

class ClientImpl;

/**
 * @brief The Client object is the main object used to establish
 * a connection with a Mofka service.
 */
class Client {

    friend class MofkaDriver;

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
     * @brief Constructor.
     *
     * @param margo_instance_id mid..
     */
    Client(margo_instance_id mid);

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
     * @brief Creates a MofkaDriver representing a Mofka service.
     *
     * @param filename name of the group file of the service.
     *
     * @return a MofkaDriver instance.
     */
    [[deprecated("Please instanciate a MofkaDriver directly")]]
    MofkaDriver connect(const std::string& groupfile) const;

    /**
     * @brief Checks that the Client instance is valid.
     */
    operator bool() const;

    /**
     * @brief Get internal configuration.
     *
     * @return configuration.
     */
    const Metadata& getConfig() const;

    private:

    Client(const std::shared_ptr<ClientImpl>& impl);

    std::shared_ptr<ClientImpl> self;
};

}

#endif
