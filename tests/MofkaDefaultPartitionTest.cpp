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

TEST_CASE("DefaultPartition test", "[default-partition]") {

    spdlog::set_level(spdlog::level::from_str("error"));

    auto remove_file = EnsureFileRemoved{"mofka.json"};

    auto server = bedrock::Server("na+sm", config);
    ENSURE(server.finalize());
    auto engine = server.getMargoManager().getThalliumEngine();

    SECTION("Initialize a client and service handle, and create topic") {
        mofka::ServiceHandle sh;
        REQUIRE(!static_cast<bool>(sh));
        REQUIRE_NOTHROW(sh = mofka::ServiceHandle{"mofka.json", engine});
        REQUIRE(static_cast<bool>(sh));

        mofka::TopicHandle topic;
        REQUIRE(!static_cast<bool>(topic));
        REQUIRE_NOTHROW(sh.createTopic("mytopic"));
        REQUIRE_THROWS_AS(sh.createTopic("mytopic"), mofka::Exception);

        SECTION("Create partition using addCustomPartition") {

            mofka::Metadata partition_config;
            mofka::ServiceHandle::PartitionDependencies partition_dependencies;
            getPartitionArguments("default", partition_dependencies, partition_config);

            REQUIRE_NOTHROW(sh.addCustomPartition(
                        "mytopic", 0, "default",
                        partition_config, partition_dependencies));

            REQUIRE_NOTHROW(topic = sh.openTopic("mytopic"));
            REQUIRE(static_cast<bool>(topic));

        }

        SECTION("Create partition using addDefaultPartition") {


            REQUIRE_NOTHROW(sh.addDefaultPartition(
                        "mytopic", 0, "my_yokan_provider@local",
                        "my_warabi_provider@local"));

            REQUIRE_NOTHROW(topic = sh.openTopic("mytopic"));
            REQUIRE(static_cast<bool>(topic));

        }

        SECTION("Let Mofka figure out the dependencies") {

            mofka::Metadata partition_config;

            REQUIRE_NOTHROW(sh.addDefaultPartition("mytopic", 0));

            REQUIRE_NOTHROW(topic = sh.openTopic("mytopic"));
            REQUIRE(static_cast<bool>(topic));

        }
    }
}
