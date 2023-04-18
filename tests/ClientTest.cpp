/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <alpha/Client.hpp>
#include <alpha/Provider.hpp>
#include <alpha/ResourceHandle.hpp>
#include <alpha/Admin.hpp>

static constexpr const char* resource_config = "{ \"path\" : \"mydb\" }";
static const std::string resource_type = "dummy";

TEST_CASE("Client test", "[client]") {

    auto engine = thallium::engine("na+sm", THALLIUM_SERVER_MODE);
    // Initialize the provider
    alpha::Provider provider(engine);
    alpha::Admin admin(engine);
    std::string addr = engine.self();
    auto resource_id = admin.createResource(addr, 0, resource_type, resource_config);

    SECTION("Open resource") {
        alpha::Client client(engine);
        std::string addr = engine.self();

        alpha::ResourceHandle my_resource = client.makeResourceHandle(addr, 0, resource_id);
        REQUIRE(static_cast<bool>(my_resource));

        auto bad_id = alpha::UUID::generate();
        REQUIRE_THROWS_AS(client.makeResourceHandle(addr, 0, bad_id), alpha::Exception);
    }

    admin.destroyResource(addr, 0, resource_id);
    engine.finalize();
}
