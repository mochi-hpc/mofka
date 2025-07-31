/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_CLIENT_HPP
#define MOFKA_CLIENT_HPP

#include "MofkaDriver.hpp"
#include "UUID.hpp"

#include <diaspora/ForwardDcl.hpp>
#include <diaspora/Json.hpp>

#include <bedrock/Client.hpp>
#include <thallium.hpp>
#include <thallium/serialization/stl/unordered_set.hpp>
#include <thallium/serialization/stl/unordered_map.hpp>
#include <thallium/serialization/stl/string.hpp>

#include <memory>
#include <string_view>

namespace mofka {


/**
 * @brief The MofkaClient object is the main object used to establish
 * a connection with a Mofka service.
 */
class MofkaClient {

    friend class MofkaDriver;

    public:

    /**
     * @brief Constructor.
     *
     * @param engine Thallium engine.
     */
    MofkaClient(const thallium::engine& engine)
    : m_engine(engine)
    , m_bedrock_client(m_engine) {}

    /**
     * @brief Copy constructor.
     */
    MofkaClient(const MofkaClient&) = default;

    /**
     * @brief Move constructor.
     */
    MofkaClient(MofkaClient&&) = default;

    /**
     * @brief Copy-assignment operator.
     */
    MofkaClient& operator=(const MofkaClient&) = default;

    /**
     * @brief Move-assignment operator.
     */
    MofkaClient& operator=(MofkaClient&&) = default;

    /**
     * @brief Destructor.
     */
    ~MofkaClient() = default;

    /**
     * @brief Returns the thallium engine used by the client.
     */
    const thallium::engine& engine() const {
        return m_engine;
    }

    private:

    thallium::engine m_engine;
    bedrock::Client  m_bedrock_client;
};

}

#endif
