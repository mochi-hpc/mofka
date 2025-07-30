/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_PROVIDER_HPP
#define MOFKA_PROVIDER_HPP

#include <diaspora/ForwardDcl.hpp>
#include <diaspora/Metadata.hpp>
#include <diaspora/Json.hpp>

#include <bedrock/AbstractComponent.hpp>
#include <thallium.hpp>
#include <memory>
#include <string_view>

namespace mofka {

class ProviderImpl;

/**
 * @brief A Provider is an object that can receive RPCs
 * and dispatch them to specific topics.
 */
class Provider {

    public:

    /**
     * @brief Constructor.
     *
     * @param engine Thallium engine to use to receive RPCs.
     * @param provider_id Provider id.
     * @param config JSON configuration.
     * @param dependencies Dependencies resolved by Bedrock or manually.
     */
    Provider(const thallium::engine& engine,
             uint16_t provider_id = 0,
             const diaspora::Metadata& config = diaspora::Metadata{"{}"},
             const bedrock::ResolvedDependencyMap& dependencies = {});

    /**
     * @brief Get the dependencies mandated by the provided configuration.
     */
    static std::vector<bedrock::Dependency> getDependencies(const diaspora::Metadata& config);

    /**
     * @brief Copy-constructor is deleted.
     */
    Provider(const Provider&) = delete;

    /**
     * @brief Move-constructor.
     */
    Provider(Provider&&);

    /**
     * @brief Copy-assignment operator is deleted.
     */
    Provider& operator=(const Provider&) = delete;

    /**
     * @brief Move-assignment operator is deleted.
     */
    Provider& operator=(Provider&&) = delete;

    /**
     * @brief Destructor.
     */
    ~Provider();

    /**
     * @brief Return a JSON configuration of the provider.
     *
     * @return JSON configuration.
     */
    const diaspora::Metadata& getConfig() const;

    /**
     * @brief Checks whether the Provider instance is valid.
     */
    operator bool() const;

    private:

    std::shared_ptr<ProviderImpl> self;
};

}

#endif
