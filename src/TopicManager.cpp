/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/TopicManager.hpp"

namespace mofka {

std::unordered_map<
    std::string,
    std::function<
        std::unique_ptr<TopicManager>(
            const thallium::engine&,
            const Metadata&,
            const Metadata&,
            const Metadata&,
            const Metadata&)>> TopicFactory::create_fn;

std::unique_ptr<TopicManager> TopicFactory::createTopic(
        std::string_view backend_name,
        const thallium::engine& engine,
        const Metadata& config,
        const Metadata& validator,
        const Metadata& selector,
        const Metadata& serializer) {
    auto it = create_fn.find(std::string{backend_name});
    if(it == create_fn.end()) return nullptr;
    auto& f = it->second;
    return f(engine, config, validator, selector, serializer);
}

}
