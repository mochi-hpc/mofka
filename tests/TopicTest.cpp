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

TEST_CASE("TopicHandle test", "[topic]") {

    spdlog::set_level(spdlog::level::from_str("critical"));
    auto remove_file = EnsureFileRemoved{"mofka.ssg"};

    auto server = bedrock::Server("na+sm", config);
    auto gid = server.getSSGManager().getGroup("mofka_group")->getHandle<uint64_t>();
    auto engine = server.getMargoManager().getThalliumEngine();

    SECTION("Initialize a Client and a ServiceHandle") {
        auto client = mofka::Client{engine};
        REQUIRE(static_cast<bool>(client));
        auto sh = client.connect(mofka::SSGGroupID{gid});
        REQUIRE(static_cast<bool>(sh));

        auto topic_config = mofka::TopicBackendConfig{R"(
            {
                "__type__":"default",
                "data_store": {
                    "__type__": "memory"
                }
            })"
        };

        SECTION("Create and open a topic") {
            mofka::TopicHandle topic;
            REQUIRE(!static_cast<bool>(topic));
            topic = sh.createTopic("mytopic", topic_config);
            REQUIRE(static_cast<bool>(topic));
            auto topic2 = sh.openTopic("mytopic");
            REQUIRE(static_cast<bool>(topic2));

            SECTION("Create a topic with a name that already exists") {
                REQUIRE_THROWS_AS(sh.createTopic("mytopic", topic_config), mofka::Exception);
            }

            SECTION("Open a topic that doesn't exist") {
                REQUIRE_THROWS_AS(sh.openTopic("abcd"), mofka::Exception);
            }
        }
    }

    server.finalize();
}
