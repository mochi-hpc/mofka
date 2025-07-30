/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "JsonUtil.hpp"
#include "Provider.hpp"

#include "PartitionManager.hpp"
#include "ProviderImpl.hpp"

#include <thallium/serialization/stl/string.hpp>

DIASPORA_INSTANTIATE_FACTORY(
    mofka::PartitionManager,
    const thallium::engine&,
    const std::string&,
    const mofka::UUID&,
    const diaspora::Metadata&,
    const bedrock::ResolvedDependencyMap&);

namespace mofka {

PartitionManagerDependencyFactory& PartitionManagerDependencyFactory::instance() {
    static PartitionManagerDependencyFactory factory;
    return factory;
}

Provider::Provider(
        const tl::engine& engine,
        uint16_t provider_id,
        const diaspora::Metadata& config,
        const bedrock::ResolvedDependencyMap& dependencies) {
    /* the pool argument is optional */
    auto it = dependencies.find("pool");
    auto pool = it != dependencies.end() ?
        it->second[0]->getHandle<tl::pool>()
        : engine.get_handler_pool();
    self = std::make_shared<ProviderImpl>(engine, provider_id, config, pool, dependencies);
    self->get_engine().push_finalize_callback(this, [p=this]() { p->self.reset(); });
}

std::vector<bedrock::Dependency> Provider::getDependencies(const diaspora::Metadata& metadata) {
    std::vector<bedrock::Dependency> dependencies;
    auto& json = metadata.json();
    if(json.is_object() && json.contains("type") && json["type"].is_string()) {
        dependencies = PartitionManagerDependencyFactory::getDependencies(
            json["type"].get_ref<const std::string&>()
        );
    }
    dependencies.push_back({"pool", "pool", false, false, false});
    dependencies.push_back({"master_database", "yokan", true, false, false});
    return dependencies;
}

Provider::Provider(Provider&& other) {
    other.self->get_engine().pop_finalize_callback(this);
    self = std::move(other.self);
    self->get_engine().push_finalize_callback(this, [p=this]() { p->self.reset(); });
}

Provider::~Provider() {
    if(self) {
        self->get_engine().pop_finalize_callback(this);
    }
}

const diaspora::Metadata& Provider::getConfig() const {
    return self->m_config;
}

Provider::operator bool() const {
    return static_cast<bool>(self);
}

}
