/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <mofka/Client.hpp>
#include <mofka/Provider.hpp>

TEST_CASE("Client test", "[client]") {

    auto engine = thallium::engine("na+sm", THALLIUM_SERVER_MODE);
    // Initialize the provider
    mofka::Provider provider(engine);

    SECTION("Initialize a client") {
        mofka::Client client;
        REQUIRE(!static_cast<bool>(client));
        client = mofka::Client{engine};
        REQUIRE(static_cast<bool>(client));
    }

    engine.finalize();
}
