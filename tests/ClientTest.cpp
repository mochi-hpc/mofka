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

    auto server = bedrock::Server("na+sm", config);
    auto engine = server.getMargoManager().getThalliumEngine();

    SECTION("Initialize a client") {
        mofka::Client client;
        REQUIRE(!static_cast<bool>(client));
        client = mofka::Client{engine};
        REQUIRE(static_cast<bool>(client));
    }

    engine.finalize();
}
