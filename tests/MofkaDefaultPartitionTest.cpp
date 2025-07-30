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

TEST_CASE("DefaultPartition test", "[default-partition]") {

    spdlog::set_level(spdlog::level::from_str("error"));

    auto remove_file = EnsureFileRemoved{"mofka.json"};

    auto server = bedrock::Server("na+sm", config);
    ENSURE(server.finalize());
    auto engine = server.getMargoManager().getThalliumEngine();

    SECTION("Initialize a client and service handle, and create topic") {
        diaspora::Driver driver;
        REQUIRE(!static_cast<bool>(driver));
        diaspora::Metadata options;
        options.json()["group_file"] = "mofka.json";
        options.json()["margo"] = nlohmann::json::object();
        options.json()["margo"]["use_progress_thread"] = true;
        driver = diaspora::Driver::New("mofka", options);
        REQUIRE(static_cast<bool>(driver));

        diaspora::TopicHandle topic;
        REQUIRE(!static_cast<bool>(topic));
        REQUIRE_NOTHROW(driver.createTopic("mytopic"));
        REQUIRE_THROWS_AS(driver.createTopic("mytopic"), diaspora::Exception);

        SECTION("Create partition using addCustomPartition") {

            diaspora::Metadata partition_config;
            mofka::MofkaDriver::Dependencies partition_dependencies;
            getPartitionArguments("default", partition_dependencies, partition_config);

            REQUIRE_NOTHROW(
                driver.as<mofka::MofkaDriver>().addCustomPartition(
                    "mytopic", 0, "default",
                    partition_config, partition_dependencies));

            REQUIRE_NOTHROW(topic = driver.openTopic("mytopic"));
            REQUIRE(static_cast<bool>(topic));

        }

        SECTION("Create partition using addDefaultPartition") {


            REQUIRE_NOTHROW(driver.as<mofka::MofkaDriver>().addDefaultPartition(
                        "mytopic", 0, "my_yokan_metadata_provider@local",
                        "my_warabi_provider@local"));

            REQUIRE_NOTHROW(topic = driver.openTopic("mytopic"));
            REQUIRE(static_cast<bool>(topic));

        }

        SECTION("Let Mofka figure out the dependencies") {

            diaspora::Metadata partition_config;

            REQUIRE_NOTHROW(driver.as<mofka::MofkaDriver>().addDefaultPartition("mytopic", 0));

            REQUIRE_NOTHROW(topic = driver.openTopic("mytopic"));
            REQUIRE(static_cast<bool>(topic));

        }

        SECTION("Create metadata and data providers") {
            std::string metadata_provider_address, data_provider_address;
            REQUIRE_NOTHROW(metadata_provider_address =
                            driver.as<mofka::MofkaDriver>().addDefaultMetadataProvider(0));
            REQUIRE_NOTHROW(data_provider_address =
                            driver.as<mofka::MofkaDriver>().addDefaultDataProvider(0));
            REQUIRE(!metadata_provider_address.empty());
            REQUIRE(!data_provider_address.empty());

            REQUIRE_NOTHROW(driver.as<mofka::MofkaDriver>().addDefaultPartition(
                        "mytopic", 0, metadata_provider_address,
                        data_provider_address));

            REQUIRE_NOTHROW(topic = driver.openTopic("mytopic"));
            REQUIRE(static_cast<bool>(topic));

        }
    }
}
