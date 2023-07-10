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

TEST_CASE("Event producer test", "[event-producer]") {

    spdlog::set_level(spdlog::level::from_str("critical"));
    auto remove_file = EnsureFileRemoved{"mofka.ssg"};

    auto server = bedrock::Server("na+sm", config);
    auto gid = server.getSSGManager().getGroup("mofka_group")->getHandle<uint64_t>();
    auto engine = server.getMargoManager().getThalliumEngine();

    SECTION("Initialize client/topic/producer") {
        auto client = mofka::Client{engine};
        REQUIRE(static_cast<bool>(client));
        auto sh = client.connect(mofka::SSGGroupID{gid});
        REQUIRE(static_cast<bool>(sh));
        mofka::TopicHandle topic;
        REQUIRE(!static_cast<bool>(topic));
        auto topic_config = mofka::TopicBackendConfig{"{\"__type__\":\"dummy\"}"};
        topic = sh.createTopic("mytopic", topic_config);
        REQUIRE(static_cast<bool>(topic));
        auto producer = topic.producer("myproducer");
        REQUIRE(static_cast<bool>(producer));

        SECTION("Push events into the topic using the producer") {
            auto future = producer.push(
                mofka::Metadata("{\"name\":\"matthieu\"}"),
                mofka::Data{nullptr, 0});
            future.wait();
        }
    }

    server.finalize();
}
