/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Provider.hpp"

#include "ProviderImpl.hpp"

#include <thallium/serialization/stl/string.hpp>

namespace mofka {

Provider::Provider(const tl::engine& engine, uint16_t provider_id, const rapidjson::Value& config, const thallium::pool& p)
: self(std::make_shared<ProviderImpl>(engine, provider_id, config, p)) {
    self->get_engine().push_finalize_callback(this, [p=this]() { p->self.reset(); });
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

const rapidjson::Value& Provider::getConfig() const {
    return self->m_config;
}

Provider::operator bool() const {
    return static_cast<bool>(self);
}

}
