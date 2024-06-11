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
#include "Configs.hpp"
#include "Ensure.hpp"

TEST_CASE("Producer test", "[producer]") {

    spdlog::set_level(spdlog::level::from_str("critical"));
    auto partition_type = GENERATE(as<std::string>{}, "memory", "default");
    CAPTURE(partition_type);
    auto remove_file = EnsureFileRemoved{"mofka_flock.json"};

    auto server = bedrock::Server("na+sm", config);
    ENSURE(server.finalize());
    auto engine = server.getMargoManager().getThalliumEngine();

    SECTION("Initialize a Client and a ServiceHandle and create/open a topic") {
        auto client = mofka::Client{engine};
        REQUIRE(static_cast<bool>(client));
        auto sh = client.connect("mofka_flock.json");
        REQUIRE(static_cast<bool>(sh));
        mofka::TopicHandle topic;
        REQUIRE(!static_cast<bool>(topic));
        REQUIRE_NOTHROW(sh.createTopic("mytopic"));
        mofka::Metadata partition_config;
        mofka::ServiceHandle::PartitionDependencies partition_dependencies;
        getPartitionArguments(partition_type, partition_dependencies, partition_config);

        REQUIRE_NOTHROW(sh.addCustomPartition(
                    "mytopic", 0, partition_type,
                    partition_config, partition_dependencies));
        REQUIRE_NOTHROW(topic = sh.openTopic("mytopic"));
        REQUIRE(static_cast<bool>(topic));

        SECTION("Create a producer from the topic") {
            mofka::Producer producer;
            REQUIRE(!static_cast<bool>(producer));
            producer = topic.producer("myproducer");
            REQUIRE(static_cast<bool>(producer));
            REQUIRE(producer.name() == "myproducer");
            REQUIRE(static_cast<bool>(producer.topic()));
            REQUIRE(producer.topic().name() == "mytopic");
        }
    }
}
