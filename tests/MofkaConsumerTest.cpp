/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <bedrock/Server.hpp>
#include <mofka/MofkaDriver.hpp>
#include <mofka/TopicHandle.hpp>
#include "Configs.hpp"
#include "Ensure.hpp"

TEST_CASE("Consumer test", "[consumer]") {

    spdlog::set_level(spdlog::level::from_str("critical"));
    auto partition_type = GENERATE(as<std::string>{}, "memory", "default");
    CAPTURE(partition_type);

    auto remove_file = EnsureFileRemoved{"mofka.json"};

    auto server = bedrock::Server("na+sm", config);
    ENSURE(server.finalize());
    auto engine = server.getMargoManager().getThalliumEngine();

    SECTION("Initialize a MofkaDriver and create/open a topic") {
        mofka::MofkaDriver driver;
        REQUIRE(!static_cast<bool>(driver));
        REQUIRE_NOTHROW(driver = mofka::MofkaDriver{"mofka.json", engine});
        REQUIRE(static_cast<bool>(driver));
        mofka::TopicHandle topic;
        REQUIRE(!static_cast<bool>(topic));
        REQUIRE_NOTHROW(driver.createTopic("mytopic"));
        REQUIRE_THROWS_AS(driver.createTopic("mytopic"), mofka::Exception);

        mofka::Metadata partition_config;
        mofka::MofkaDriver::Dependencies partition_dependencies;
        getPartitionArguments(partition_type, partition_dependencies, partition_config);

        REQUIRE_NOTHROW(driver.addCustomPartition(
                    "mytopic", 0, partition_type,
                    partition_config, partition_dependencies));

        REQUIRE_NOTHROW(topic = driver.openTopic("mytopic"));
        REQUIRE(static_cast<bool>(topic));
        REQUIRE_THROWS_AS(driver.openTopic("mytopic2"), mofka::Exception);

        SECTION("Create a consumer from the topic") {
            mofka::Consumer consumer;
            REQUIRE(!static_cast<bool>(consumer));
            REQUIRE_NOTHROW(consumer = topic.consumer("myconsumer"));
            REQUIRE(static_cast<bool>(consumer));
            REQUIRE(consumer.name() == "myconsumer");
            REQUIRE(static_cast<bool>(consumer.topic()));
            REQUIRE(consumer.topic().name() == "mytopic");
        }
    }
}
