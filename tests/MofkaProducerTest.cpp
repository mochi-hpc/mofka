/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <bedrock/Server.hpp>
#include <diaspora/Driver.hpp>
#include <diaspora/TopicHandle.hpp>
#include "../src/MofkaDriver.hpp"
#include "Configs.hpp"
#include "Ensure.hpp"

TEST_CASE("Producer test", "[producer]") {

    spdlog::set_level(spdlog::level::from_str("critical"));
    auto partition_type = GENERATE(as<std::string>{}, "memory", "default");
    CAPTURE(partition_type);
    auto remove_file = EnsureFileRemoved{"mofka.json"};

    auto server = bedrock::Server("na+sm", config);
    ENSURE(server.finalize());
    auto engine = server.getMargoManager().getThalliumEngine();

    SECTION("Initialize and a MofkaDriver and create/open a topic") {

        diaspora::Metadata options;
        options.json()["group_file"] = "mofka.json";
        options.json()["margo"] = nlohmann::json::object();
        options.json()["margo"]["use_progress_thread"] = true;
        diaspora::Driver driver = diaspora::Driver::New("mofka", options);
        REQUIRE(static_cast<bool>(driver));

        diaspora::TopicHandle topic;
        REQUIRE(!static_cast<bool>(topic));
        REQUIRE_NOTHROW(driver.createTopic("mytopic"));
        diaspora::Metadata partition_config;
        mofka::MofkaDriver::Dependencies partition_dependencies;
        getPartitionArguments(partition_type, partition_dependencies, partition_config);

        REQUIRE_NOTHROW(driver.as<mofka::MofkaDriver>().addCustomPartition(
                    "mytopic", 0, partition_type,
                    partition_config, partition_dependencies));
        REQUIRE_NOTHROW(topic = driver.openTopic("mytopic"));
        REQUIRE(static_cast<bool>(topic));

        SECTION("Create a producer from the topic") {
            diaspora::Producer producer;
            REQUIRE(!static_cast<bool>(producer));
            producer = topic.producer("myproducer");
            REQUIRE(static_cast<bool>(producer));
            REQUIRE(producer.name() == "myproducer");
            REQUIRE(static_cast<bool>(producer.topic()));
            REQUIRE(producer.topic().name() == "mytopic");
        }
    }
}
