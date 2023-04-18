/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <alpha/Admin.hpp>
#include <alpha/Provider.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>

static const std::string resource_type = "dummy";
static constexpr const char* resource_config = "{ \"path\" : \"mydb\" }";

TEST_CASE("Admin tests", "[admin]") {

    auto engine = thallium::engine("na+sm", THALLIUM_SERVER_MODE);
    // Initialize the provider
    alpha::Provider provider(engine);

    SECTION("Create an admin") {
        alpha::Admin admin(engine);
        std::string addr = engine.self();

        SECTION("Create and destroy resources") {
            alpha::UUID resource_id = admin.createResource(addr, 0, resource_type, resource_config);

            REQUIRE_THROWS_AS(admin.createResource(addr, 0, "blabla", resource_config),
                              alpha::Exception);

            admin.destroyResource(addr, 0, resource_id);

            alpha::UUID bad_id;
            REQUIRE_THROWS_AS(admin.destroyResource(addr, 0, bad_id), alpha::Exception);
        }
    }
    // Finalize the engine
    engine.finalize();
}
