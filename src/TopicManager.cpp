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
        std::unique_ptr<TopicManager>(const thallium::engine&, const rapidjson::Value&)>> TopicFactory::create_fn;

std::unique_ptr<TopicManager> TopicFactory::createTopic(std::string_view backend_name,
                                                        const thallium::engine& engine,
                                                        const rapidjson::Value& config) {
    auto it = create_fn.find(std::string{backend_name});
    if(it == create_fn.end()) return nullptr;
    auto& f = it->second;
    return f(engine, config);
}

}
