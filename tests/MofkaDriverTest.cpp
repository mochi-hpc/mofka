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
#include "Configs.hpp"
#include "Ensure.hpp"

TEST_CASE("Mofka driver test", "[driver]") {

    spdlog::set_level(spdlog::level::from_str("error"));
    auto partition_type = GENERATE(as<std::string>{}, "memory", "default");
    CAPTURE(partition_type);

    auto remove_file = EnsureFileRemoved{"mofka.json"};

    auto server = bedrock::Server("na+sm", config);
    ENSURE(server.finalize());
    auto engine = server.getMargoManager().getThalliumEngine();

    SECTION("Initialize a service handle") {
        diaspora::Driver driver;
        diaspora::Metadata options;
        options.json()["group_file"] = "mofka.json";
        options.json()["margo"] = nlohmann::json::object();
        options.json()["margo"]["use_progress_thread"] = true;
        driver = diaspora::Driver::New("mofka", options);
        REQUIRE(static_cast<bool>(driver));

        SECTION("Create a topic") {
            diaspora::TopicHandle topic;
            REQUIRE(!static_cast<bool>(topic));
            REQUIRE(!driver.topicExists("mytopic"));
            REQUIRE_NOTHROW(driver.createTopic("mytopic"));
            REQUIRE(driver.topicExists("mytopic"));
            REQUIRE_THROWS_AS(driver.createTopic("mytopic"), diaspora::Exception);

            diaspora::Metadata partition_config;
            mofka::MofkaDriver::Dependencies partition_dependencies;
            getPartitionArguments(partition_type, partition_dependencies, partition_config);

            for(size_t i = 0; i < 4; ++i) {
                REQUIRE_NOTHROW(driver.as<mofka::MofkaDriver>().addCustomPartition(
                                "mytopic", 0, partition_type,
                                partition_config, partition_dependencies));
            }

            REQUIRE_NOTHROW(topic = driver.openTopic("mytopic"));
            REQUIRE(static_cast<bool>(topic));
            REQUIRE_THROWS_AS(driver.openTopic("mytopic2"), diaspora::Exception);

            REQUIRE(topic.partitions().size() == 4);

            //std::cerr << "---------------------------------------------" << std::endl;
            //std::cerr << server.getCurrentConfig() << std::endl;
        }
    }
}
