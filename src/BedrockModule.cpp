/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "Provider.hpp"

#include <bedrock/AbstractComponent.hpp>

#include <memory>

namespace tl = thallium;

class MofkaComponent : public bedrock::AbstractComponent {

    std::unique_ptr<mofka::Provider> m_provider;

    public:

    MofkaComponent(const tl::engine& engine,
                   uint16_t  provider_id,
                   const diaspora::Metadata& config,
                   const bedrock::ResolvedDependencyMap& dependencies)
    : m_provider{std::make_unique<mofka::Provider>(engine, provider_id, config, dependencies)}
    {}

    void* getHandle() override {
        return static_cast<void*>(m_provider.get());
    }

    std::string getConfig() override {
        return m_provider->getConfig().string();
    }

    static std::shared_ptr<bedrock::AbstractComponent>
        Register(const bedrock::ComponentArgs& args) {
            auto config = diaspora::Metadata{args.config, true};
            return std::make_shared<MofkaComponent>(
                args.engine, args.provider_id, config, args.dependencies);
        }

    static std::vector<bedrock::Dependency>
        GetDependencies(const bedrock::ComponentArgs& args) {
            auto config_metadata = diaspora::Metadata{args.config, true};
            return mofka::Provider::getDependencies(config_metadata);
        }
};

BEDROCK_REGISTER_COMPONENT_TYPE(mofka, MofkaComponent)
