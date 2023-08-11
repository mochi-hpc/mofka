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

TEST_CASE("Event consumer test", "[event-consumer]") {

    spdlog::set_level(spdlog::level::from_str("critical"));
    auto remove_file = EnsureFileRemoved{"mofka.ssg"};

    auto server = bedrock::Server("na+sm", config);
    auto gid = server.getSSGManager().getGroup("mofka_group")->getHandle<uint64_t>();
    auto engine = server.getMargoManager().getThalliumEngine();

    SECTION("Producer/consumer") {
        auto client = mofka::Client{engine};
        REQUIRE(static_cast<bool>(client));
        auto sh = client.connect(mofka::SSGGroupID{gid});
        REQUIRE(static_cast<bool>(sh));
        mofka::TopicHandle topic;
        REQUIRE(!static_cast<bool>(topic));
        auto topic_config = mofka::TopicBackendConfig{"{\"__type__\":\"dummy\"}"};
        topic = sh.createTopic("mytopic", topic_config);
        REQUIRE(static_cast<bool>(topic));

        {
            auto producer = topic.producer();
            REQUIRE(static_cast<bool>(producer));
            for(unsigned i=0; i < 100; ++i) {
                mofka::Metadata metadata = mofka::Metadata{
                    fmt::format("{{\"event_num\":{}}}", i)
                };
                producer.push(metadata);
            }
        }

        {
            auto consumer = topic.consumer("myconsumer");
            REQUIRE(static_cast<bool>(consumer));
            for(unsigned i=0; i < 100; ++i) {
                auto event = consumer.pull().wait();
                REQUIRE(event.id() == i);
                auto& doc = event.metadata().json();
                REQUIRE(doc["event_num"].GetInt64() == i);
            }
        }
    }

    server.finalize();
}
