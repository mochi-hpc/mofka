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
        rapidjson::Document config;
        rapidjson::ParseResult ok = config.Parse(args.config.c_str());
        if(!ok) {
            // TODO print error
            return nullptr;
        }
        auto provider = new mofka::Provider(
            args.mid, args.provider_id,
            config, tl::pool(args.pool));
        return static_cast<void *>(provider);
    }

    void deregisterProvider(void *p) override {
        auto provider = static_cast<mofka::Provider *>(p);
        delete provider;
    }

    std::string getProviderConfig(void *p) override {
        auto provider = static_cast<mofka::Provider *>(p);
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        provider->getConfig().Accept(writer);
        return buffer.GetString();
    }

    void *initClient(const bedrock::FactoryArgs& args) override {
        return static_cast<void *>(new mofka::Client(args.mid));
    }

    void finalizeClient(void *client) override {
        delete static_cast<mofka::Client *>(client);
    }

    std::string getClientConfig(void* c) override {
        auto client = static_cast<mofka::Client*>(c);
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        client->getConfig().Accept(writer);
        return buffer.GetString();
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

    const std::vector<bedrock::Dependency> &getProviderDependencies() override {
        static const std::vector<bedrock::Dependency> no_dependency;
        return no_dependency;
    }

    const std::vector<bedrock::Dependency> &getClientDependencies() override {
        static const std::vector<bedrock::Dependency> no_dependency;
        return no_dependency;
    }
};

BEDROCK_REGISTER_MODULE_FACTORY(mofka, MofkaFactory)
