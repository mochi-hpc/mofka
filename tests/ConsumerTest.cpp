/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <bedrock/Server.hpp>
#include <mofka/Client.hpp>
#include <mofka/TopicHandle.hpp>
#include "BedrockConfig.hpp"

TEST_CASE("Consumer test", "[consumer]") {

    spdlog::set_level(spdlog::level::from_str("critical"));
    auto remove_file = EnsureFileRemoved{"mofka.ssg"};

    auto server = bedrock::Server("na+sm", config);
    auto gid = server.getSSGManager().getGroup("mofka_group")->getHandle<uint64_t>();
    auto engine = server.getMargoManager().getThalliumEngine();

    SECTION("Initialize a Client and a ServiceHandle and create/open a topic") {
        auto client = mofka::Client{engine};
        REQUIRE(static_cast<bool>(client));
        auto sh = client.connect(mofka::SSGGroupID{gid});
        REQUIRE(static_cast<bool>(sh));
        mofka::TopicHandle topic;
        REQUIRE(!static_cast<bool>(topic));
        auto topic_config = mofka::TopicBackendConfig{};/*R"(
            {
                "__type__":"default",
                "data_store": {
                    "__type__": "memory"
                }
            })"
        };*/
        topic = sh.createTopic("mytopic", topic_config);
        REQUIRE(static_cast<bool>(topic));

        SECTION("Create a consumer from the topic") {
            mofka::Consumer consumer;
            REQUIRE(!static_cast<bool>(consumer));
            consumer = topic.consumer("myconsumer");
            REQUIRE(static_cast<bool>(consumer));
            REQUIRE(consumer.name() == "myconsumer");
            REQUIRE(static_cast<bool>(consumer.topic()));
            REQUIRE(consumer.topic().name() == "mytopic");
        }
    }

    server.finalize();
}
