/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Client.hpp"
#include "mofka/Provider.hpp"
#include "mofka/ProviderHandle.hpp"
#include <bedrock/AbstractServiceFactory.hpp>

namespace tl = thallium;

class MofkaFactory : public bedrock::AbstractServiceFactory {

    public:

    MofkaFactory() {}

    void *registerProvider(const bedrock::FactoryArgs &args) override {
        mofka::Metadata config;
        try {
            config = mofka::Metadata{args.config.c_str(), true};
        } catch(const mofka::Exception& ex) {
            spdlog::error("Error parsing configuration for Mofka provider: {}", ex.what());
            return nullptr;
        }
        auto provider = new mofka::Provider(
            args.mid, args.provider_id,
            config, tl::pool(args.pool),
            args.dependencies);
        return static_cast<void *>(provider);
    }

    void deregisterProvider(void *p) override {
        auto provider = static_cast<mofka::Provider *>(p);
        delete provider;
    }

    std::string getProviderConfig(void *p) override {
        auto provider = static_cast<mofka::Provider *>(p);
        return provider->getConfig().string();
    }

    void *initClient(const bedrock::FactoryArgs& args) override {
        return static_cast<void *>(new mofka::Client(args.mid));
    }

    void finalizeClient(void *client) override {
        delete static_cast<mofka::Client *>(client);
    }

    std::string getClientConfig(void* c) override {
        auto client = static_cast<mofka::Client*>(c);
        return client->getConfig().string();
    }

    void *createProviderHandle(void *c, hg_addr_t address,
                               uint16_t provider_id) override {
        auto client = static_cast<mofka::Client *>(c);
        auto ph = new mofka::ProviderHandle(
                client->engine(),
                address,
                provider_id,
                false);
        return static_cast<void *>(ph);
    }

    void destroyProviderHandle(void *providerHandle) override {
        auto ph = static_cast<mofka::ProviderHandle *>(providerHandle);
        delete ph;
    }

    std::vector<bedrock::Dependency> getProviderDependencies(const char* config) override {
        auto config_metadata = mofka::Metadata{config, true};
        return mofka::Provider::getDependencies(config_metadata);
    }

    const std::vector<bedrock::Dependency> &getClientDependencies() override {
        static const std::vector<bedrock::Dependency> no_dependency;
        return no_dependency;
    }
};

BEDROCK_REGISTER_MODULE_FACTORY(mofka, MofkaFactory)
