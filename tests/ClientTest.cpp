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

TEST_CASE("Client test", "[client]") {

    spdlog::set_level(spdlog::level::from_str("error"));
    auto partition_type = GENERATE(as<std::string>{}, "memory", "default");
    CAPTURE(partition_type);

    auto remove_file = EnsureFileRemoved{"mofka.ssg"};

    auto server = bedrock::Server("na+sm", config);
    ENSURE(server.finalize());
    auto gid = server.getSSGManager().getGroup("mofka_group")->getHandle<uint64_t>();
    auto engine = server.getMargoManager().getThalliumEngine();

    SECTION("Initialize a client") {
        mofka::Client client;
        REQUIRE(!static_cast<bool>(client));
        REQUIRE_NOTHROW(client = mofka::Client{engine});
        REQUIRE(static_cast<bool>(client));

        SECTION("Initialize a service handle") {
            mofka::ServiceHandle sh;
            REQUIRE(!static_cast<bool>(sh));
            REQUIRE_NOTHROW(sh = client.connect(mofka::SSGGroupID{gid}));
            REQUIRE(static_cast<bool>(sh));

            SECTION("Create a topic") {
                mofka::TopicHandle topic;
                REQUIRE(!static_cast<bool>(topic));
                REQUIRE_NOTHROW(sh.createTopic("mytopic"));
                REQUIRE_THROWS_AS(sh.createTopic("mytopic"), mofka::Exception);

                mofka::Metadata partition_config;
                mofka::ServiceHandle::PartitionDependencies partition_dependencies;
                getPartitionArguments(partition_type, partition_dependencies, partition_config);

                REQUIRE_NOTHROW(sh.addCustomPartition(
                        "mytopic", 0, partition_type,
                        partition_config, partition_dependencies));

                REQUIRE_NOTHROW(topic = sh.openTopic("mytopic"));
                REQUIRE(static_cast<bool>(topic));
                REQUIRE_THROWS_AS(sh.openTopic("mytopic2"), mofka::Exception);
            }
        }
    }
}
