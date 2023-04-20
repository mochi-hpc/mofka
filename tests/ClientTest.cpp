/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <bedrock/Server.hpp>
#include <mofka/Client.hpp>
#include "BedrockConfig.hpp"

TEST_CASE("Client test", "[client]") {

    spdlog::set_level(spdlog::level::from_str("critical"));
    auto remove_file = EnsureFileRemoved{"mofka.ssg"};

    auto server = bedrock::Server("na+sm", config);
    auto gid = server.getSSGManager().getGroup("mofka_group")->getHandle<uint64_t>();
    auto engine = server.getMargoManager().getThalliumEngine();

    SECTION("Initialize a client") {
        mofka::Client client;
        REQUIRE(!static_cast<bool>(client));
        client = mofka::Client{engine};
        REQUIRE(static_cast<bool>(client));

        SECTION("Initialize a service handle") {
            mofka::ServiceHandle sh;
            REQUIRE(!static_cast<bool>(sh));
            sh = client.connect(mofka::SSGGroupID{gid});
            REQUIRE(static_cast<bool>(sh));
        }
    }

    server.finalize();
}
