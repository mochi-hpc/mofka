/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "JsonUtil.hpp"
#include "mofka/Provider.hpp"

#include "ProviderImpl.hpp"

#include <thallium/serialization/stl/string.hpp>

namespace mofka {

Provider::Provider(
        const tl::engine& engine,
        uint16_t provider_id,
        const Metadata& config,
        const bedrock::ResolvedDependencyMap& dependencies) {
    /* the pool argument is optional */
    auto it = dependencies.find("pool");
    auto pool = it != dependencies.end() ?
        it->second[0]->getHandle<tl::pool>()
        : engine.get_handler_pool();
    self = std::make_shared<ProviderImpl>(engine, provider_id, config, pool, dependencies);
    self->get_engine().push_finalize_callback(this, [p=this]() { p->self.reset(); });
}

std::vector<bedrock::Dependency> Provider::getDependencies(const Metadata& metadata) {
    std::vector<bedrock::Dependency> dependencies;
    auto& json = metadata.json();
    if(json.is_object() && json.contains("type") && json["type"].is_string()) {
        dependencies = PartitionManagerDependencyFactory::getDependencies(
            json["type"].get_ref<const std::string&>()
        );
    }
    dependencies.push_back({"pool", "pool", false, false, false});
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

const Metadata& Provider::getConfig() const {
    return self->m_config;
}

Provider::operator bool() const {
    return static_cast<bool>(self);
}

}
