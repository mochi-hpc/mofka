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

TEST_CASE("Event producer test", "[event-producer]") {

    spdlog::set_level(spdlog::level::from_str("critical"));
    auto partition_type = GENERATE(as<std::string>{}, "memory", "default");
    CAPTURE(partition_type);
    auto remove_file = EnsureFileRemoved{"mofka.json"};

    auto server = bedrock::Server("na+sm", config);
    ENSURE(server.finalize());
    auto engine = server.getMargoManager().getThalliumEngine();

    SECTION("Initialize client/topic/producer") {
        auto driver = mofka::MofkaDriver{"mofka.json", engine};
        REQUIRE(static_cast<bool>(driver));
        mofka::TopicHandle topic;
        REQUIRE(!static_cast<bool>(topic));
        REQUIRE_NOTHROW(driver.createTopic("mytopic"));
        mofka::Metadata partition_config;
        mofka::MofkaDriver::PartitionDependencies partition_dependencies;
        getPartitionArguments(partition_type, partition_dependencies, partition_config);

        REQUIRE_NOTHROW(driver.addCustomPartition(
                    "mytopic", 0, partition_type,
                    partition_config, partition_dependencies));
        REQUIRE_NOTHROW(topic = driver.openTopic("mytopic"));
        REQUIRE(static_cast<bool>(topic));

        auto thread_count = GENERATE(as<mofka::ThreadCount>{}, 0, 1, 2);
        auto batch_size   = GENERATE(mofka::BatchSize::Adaptive(), mofka::BatchSize::Adaptive());
        auto ordering     = GENERATE(mofka::Ordering::Strict, mofka::Ordering::Loose);

        auto producer = topic.producer(
            "myproducer", batch_size, thread_count, ordering);
        REQUIRE(static_cast<bool>(producer));

        {
            mofka::Future<mofka::EventID> future;
            REQUIRE_NOTHROW(future = producer.push(
                mofka::Metadata("{\"name\":\"matthieu\"}"),
                mofka::Data{nullptr, 0}));
            REQUIRE_NOTHROW(producer.flush());
            REQUIRE_NOTHROW(future.wait());
        }

        {
            std::string someData = "This is some data";
            mofka::Future<mofka::EventID> future;
            REQUIRE_NOTHROW(future = producer.push(
                mofka::Metadata("{\"name\":\"matthieu\"}"),
                mofka::Data{someData.data(), someData.size()}));
            REQUIRE_NOTHROW(producer.flush());
            REQUIRE_NOTHROW(future.wait());
        }
    }
}
